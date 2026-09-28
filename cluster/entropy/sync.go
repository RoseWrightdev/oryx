// Package entropy performs background anti-entropy state reconciliation and sync protocols.
package entropy

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"sync"
	"time"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/cluster/gateway"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/core/hashmap"
	"github.com/rosewrightdev/oryx/kv"
	"google.golang.org/grpc/credentials"
)

// StateWriter defines the interface for applying sets and deletes to the state.
type StateWriter interface {
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
}

// Syncer performs periodic state reconciliation between nodes.
// It detects divergence in storage shards and pulls missing data from peers.
type Syncer struct {
	writer     StateWriter
	mesh       mesh.Mesher
	creds      credentials.TransportCredentials
	meshConfig *mesh.Config
	hm         *hashmap.ShardedMap
	pools      *pools
	stopChan   chan struct{}
	stopOnce   sync.Once
	cc         *gateway.ClientCache
	nodeID     kv.NodeID
	interval   time.Duration
}

// SyncerConfig holds configuration options for the Syncer service.
type SyncerConfig struct {
	Writer     StateWriter
	Mesh       mesh.Mesher
	Creds      credentials.TransportCredentials
	MeshConfig *mesh.Config
	Hm         *hashmap.ShardedMap
	Cc         *gateway.ClientCache
	NodeID     kv.NodeID
	Interval   time.Duration
}

type pools struct {
	shardDigests sync.Pool
	shardMaps    sync.Pool
	bucketMaps   sync.Pool
	pullRequests sync.Pool
}

// NewSyncer initializes a new Syncer instance.
func NewSyncer(config *SyncerConfig) *Syncer {
	if config.Mesh == nil {
		panic("Syncer requires a mesh implementation")
	}
	if config.Cc == nil {
		panic("Syncer requires a non-nil ClientCache")
	}

	return &Syncer{
		writer:     config.Writer,
		mesh:       config.Mesh,
		meshConfig: config.MeshConfig,
		hm:         config.Hm,
		nodeID:     config.NodeID,
		interval:   config.Interval,
		creds:      config.Creds,
		cc:         config.Cc,
		stopChan:   make(chan struct{}),
		pools: &pools{
			shardDigests: sync.Pool{
				New: func() any { return &pb.ShardDigests{} },
			},
			shardMaps: sync.Pool{
				New: func() any { return make(map[hashmap.ShardID]hashmap.Digest) },
			},
			bucketMaps: sync.Pool{
				New: func() any {
					m := make(map[hashmap.ShardID]hashmap.ShardDigest, hashmap.ShardCount)
					for i := range hashmap.ShardCount {
						m[hashmap.ShardID(i)] = make([]hashmap.Digest, hashmap.SubBucketCount)
					}
					return m
				},
			},
			pullRequests: sync.Pool{
				New: func() any {
					return &pb.PullRequest{
						ShardDigests: make(map[uint32]uint64, hashmap.ShardCount),
						SubDigests:   make(map[uint32]*pb.ShardDigests, hashmap.ShardCount),
					}
				},
			},
		},
	}
}

// Start begins the background reconciliation loop until stopChan is closed.
func (s *Syncer) Start() {
	if s.interval <= 0 {
		panic(fmt.Sprintf("invalid Syncer interval: %v", s.interval))
	}
	go s.run()
}

// Stop gracefully terminates the background reconciliation loop.
// It is safe to call Stop concurrently or multiple times.
func (s *Syncer) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopChan)
	})
}

func (s *Syncer) run() {
	slog.Info("Syncer service started", "interval", s.interval)

	// Reconcile immediately rather than waiting for the first tick, so a
	// freshly joined node isn't divergent for a whole interval (#63).
	select {
	case <-s.stopChan:
		return
	default:
		s.performSync()
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.performSync()
		}
	}
}

// Push applies a batch of sets and deletes directly to the local state writer.
func (s *Syncer) Push(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	for _, req := range sets {
		if err := s.writer.ApplySet(req); err != nil {
			return err
		}
	}
	for _, d := range deletes {
		if err := s.writer.ApplyDelete(d); err != nil {
			return err
		}
	}
	return nil
}

// PullConfig contains metadata for requesting state reconciliation.
type PullConfig struct {
	Shards      map[hashmap.ShardID]hashmap.Digest
	Buckets     map[hashmap.ShardID]hashmap.ShardDigest
	RequesterID kv.NodeID
	Root        hashmap.RootDigest
}

// Pull compares remote shard and bucket digests against local state and returns mismatch records.
func (s *Syncer) Pull(pullConfig *PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	// Level 1: Global check. If the root hash matches, the entire database is identical.
	if pullConfig.Root == s.hm.RootDigest() {
		return nil, nil, nil
	}

	localShardDigests := s.pools.shardMaps.Get().(map[hashmap.ShardID]hashmap.Digest)
	localBuckets := s.pools.bucketMaps.Get().(map[hashmap.ShardID]hashmap.ShardDigest)
	defer func() {
		s.pools.shardMaps.Put(localShardDigests)
		s.pools.bucketMaps.Put(localBuckets)
	}()

	s.hm.FillShardDigests(localShardDigests)
	s.hm.FillDigests(localBuckets)

	var sets []*pb.SetRequest
	var deletes []*pb.DeleteRequest

	for shardID, localShardHash := range localShardDigests {
		remoteShardHash, hasShard := pullConfig.Shards[shardID]

		// Level 2: Shard check
		if hasShard && localShardHash == remoteShardHash {
			continue
		}

		remoteBucketHashes, hasBuckets := pullConfig.Buckets[shardID]
		localBucketHashes := localBuckets[shardID]

		// Level 3: Determine which sub-buckets need syncing using a bitmask.
		mismatchMask := calculateMismatchMask(hasBuckets, remoteBucketHashes, localBucketHashes)

		if mismatchMask > 0 {
			sets, deletes = s.collectShardMismatches(shardID, mismatchMask, pullConfig.RequesterID, sets, deletes)
		}
	}
	return sets, deletes, nil
}

// collectShardMismatches iterates over mismatched sub-buckets in a shard and collects changed keys.
func (s *Syncer) collectShardMismatches(
	shardID hashmap.ShardID,
	mismatchMask uint64,
	requesterID kv.NodeID,
	sets []*pb.SetRequest,
	deletes []*pb.DeleteRequest,
) ([]*pb.SetRequest, []*pb.DeleteRequest) {
	s.hm.RangeShard(shardID, mismatchMask, func(k kv.Key, v kv.Value) {
		if !s.isRequesterResponsible(k, requesterID) {
			return
		}

		if v.Tombstone {
			deletes = append(deletes, s.buildDeleteRequest(k, v))
		} else {
			sets = append(sets, s.buildSetRequest(k, v))
		}
	})
	return sets, deletes
}

// calculateMismatchMask returns a bitmask representing mismatched sub-buckets (0-63).
func calculateMismatchMask(hasBuckets bool, remoteBucketHashes, localBucketHashes hashmap.ShardDigest) uint64 {
	if !hasBuckets || len(remoteBucketHashes) != len(localBucketHashes) {
		return ^uint64(0)
	}
	var mismatchMask uint64
	for i := range hashmap.SubBucketCount {
		if localBucketHashes[i] != remoteBucketHashes[i] {
			// Use uint64(1) to avoid untyped-int shift overflow on 32-bit
			// platforms when i >= 32 (#90).
			mismatchMask |= uint64(1) << i
		}
	}
	return mismatchMask
}

// isRequesterResponsible returns true if the remote node is responsible for replicating the key.
func (s *Syncer) isRequesterResponsible(key string, requesterID kv.NodeID) bool {
	if s.meshConfig.SingleNode {
		return true
	}
	owners := s.mesh.GetOwners(kv.Key(key), s.meshConfig.ReplicationFactor)
	return slices.Contains(owners, requesterID)
}

// buildSetRequest builds a Pull response entry. Not pool-leased: it's
// returned straight to gRPC and nothing ever calls Put on it (#66).
func (s *Syncer) buildSetRequest(key string, val kv.Value) *pb.SetRequest {
	return &pb.SetRequest{
		Key:       key,
		Value:     val.Data,
		Timestamp: val.Timestamp,
		NodeId:    val.NodeID,
	}
}

// buildDeleteRequest is the delete-tombstone counterpart to buildSetRequest.
func (s *Syncer) buildDeleteRequest(key string, val kv.Value) *pb.DeleteRequest {
	return &pb.DeleteRequest{
		Key:       key,
		Timestamp: val.Timestamp,
		NodeId:    val.NodeID,
	}
}

// excludeSelf removes self from members. Mesher.Members() includes the
// caller, and syncing against self is a guaranteed no-op.
func excludeSelf(members []mesh.PeerAddress, self mesh.PeerAddress) []mesh.PeerAddress {
	peers := members[:0:0]
	for _, addr := range members {
		if addr != self {
			peers = append(peers, addr)
		}
	}
	return peers
}

func (s *Syncer) performSync() {
	members := s.mesh.Members()
	if len(members) == 0 {
		return
	}

	peers := excludeSelf(members, s.mesh.AddressForNode(s.nodeID))
	if len(peers) == 0 {
		return
	}

	// #nosec G404
	target := peers[rand.Intn(len(peers))]
	client, err := s.cc.Get(target)
	if err != nil {
		slog.Debug("Failed to connect to peer for Syncer sync", "peer", target, "error", err)
		return
	}

	req := s.pools.pullRequests.Get().(*pb.PullRequest)
	localShards, localBuckets := s.preparePullRequest(req)
	defer func() {
		s.pools.shardMaps.Put(localShards)
		s.pools.bucketMaps.Put(localBuckets)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.API.Pull(ctx, req)
	s.cleanupPullRequest(req)

	if err != nil {
		slog.Error("Syncer pull failed", "peer", target, "error", err)
		return
	}

	if len(resp.Entries) > 0 || len(resp.Deletions) > 0 {
		slog.Info("Syncer detected divergence and synced state",
			"peer", target, "sets", len(resp.Entries), "deletes", len(resp.Deletions))

		if err := s.Push(resp.Entries, resp.Deletions); err != nil {
			slog.Error("Failed to apply synced state from Syncer", "error", err)
		}
	}
}

func (s *Syncer) preparePullRequest(req *pb.PullRequest) (map[hashmap.ShardID]hashmap.Digest, map[hashmap.ShardID]hashmap.ShardDigest) {
	localShards := s.pools.shardMaps.Get().(map[hashmap.ShardID]hashmap.Digest)
	localBuckets := s.pools.bucketMaps.Get().(map[hashmap.ShardID]hashmap.ShardDigest)

	s.hm.FillShardDigests(localShards)
	s.hm.FillDigests(localBuckets)

	req.NodeId = string(s.nodeID)
	req.RootDigest = uint64(s.hm.RootDigest())

	// Prepare Shard Digests
	for k := range req.ShardDigests {
		delete(req.ShardDigests, k)
	}
	for id, h := range localShards {
		// #nosec G115
		idU := uint32(id)
		req.ShardDigests[idU] = uint64(h)
	}

	// Prepare Sub-Bucket Digests
	for k := range req.SubDigests {
		delete(req.SubDigests, k)
	}
	for id, hashes := range localBuckets {
		sd := s.pools.shardDigests.Get().(*pb.ShardDigests)
		sd.SubHashes = hashes
		// #nosec G115
		idU := uint32(id)
		req.SubDigests[idU] = sd
	}

	return localShards, localBuckets
}

func (s *Syncer) cleanupPullRequest(req *pb.PullRequest) {
	for _, sd := range req.SubDigests {
		sd.SubHashes = nil
		s.pools.shardDigests.Put(sd)
	}
	for k := range req.SubDigests {
		delete(req.SubDigests, k)
	}
	s.pools.pullRequests.Put(req)
}
