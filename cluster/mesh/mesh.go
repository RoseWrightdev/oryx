package mesh

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/kv"
	"google.golang.org/protobuf/proto"
)

// PeerAddress represents the network address (IP:Port) of a oryx node.
type PeerAddress string

// Gossiper defines the interface for handling incoming gossip messages.
type Gossiper interface {
	OnGossip(b []byte)
}

// Mesher defines the interface for distributed node discovery and replication.
type Mesher interface {
	Broadcast(msg []byte)
	Members() []PeerAddress
	Owner(key kv.Key) kv.NodeID
	GetOwners(key kv.Key, n int) []kv.NodeID
	AddressForNode(nodeID kv.NodeID) PeerAddress
	Start() error
	Stop() error
	UpdateLocalWeight(weight int)
	// LocalGossipPort returns the actual port bound for gossip, which can
	// differ from the configured port when it was 0 (dynamic allocation).
	LocalGossipPort() int
}

// Config holds configuration for decentralized node discovery and membership.
type Config struct {
	NodeID                 kv.NodeID
	BindAddr               string
	AdvertiseAddr          string
	SeedNodes              []string
	ReplicationFactor      int
	BindPort               int
	GrpcPort               int
	SingleNode             bool
	FastTest               bool
	ReplicationFailureMode ReplicationFailureMode
	// JoinRetries bounds Start's memberlist.Join retries. Zero uses defaultJoinRetries (#93).
	JoinRetries int
	// JoinRetryBaseDelay is the initial backoff, doubling per retry. Zero uses defaultJoinRetryBaseDelay.
	JoinRetryBaseDelay time.Duration
}

type ReplicationFailureMode string

const (
	Strict  ReplicationFailureMode = "strict"
	Lenient ReplicationFailureMode = "lenient"
)

const (
	defaultJoinRetries        = 5
	defaultJoinRetryBaseDelay = 500 * time.Millisecond
	maxJoinRetryDelay         = 10 * time.Second
)

// Mesh provides the implementation for L7 Routing and P2P communication between nodes.
type Mesh struct {
	gossip      Gossiper
	memberList  *memberlist.Memberlist
	broadcasts  *memberlist.TransmitLimitedQueue
	ring        *HashRing
	nodeAddrs   sync.Map
	config      Config
	stopping    atomic.Bool
	localWeight atomic.Int32
}

// NewMesh initializes a new Mesh instance.
func NewMesh(gossip Gossiper, config Config) (*Mesh, error) {
	ring := NewHashRing()
	m := &Mesh{
		gossip: gossip,
		config: config,
		ring:   ring,
	}
	m.localWeight.Store(defaultVnodes)

	mlConfig := memberlist.DefaultLocalConfig()
	if config.FastTest {
		mlConfig = memberlist.DefaultLANConfig()
		mlConfig.PushPullInterval = 500 * time.Millisecond
		mlConfig.GossipInterval = 20 * time.Millisecond
		mlConfig.ProbeInterval = 100 * time.Millisecond
		mlConfig.SuspicionMult = 2
	}
	// Route memberlist diagnostic logs through slog instead of discarding
	// them. Discarding memberlist logs hides critical network partition,
	// node failure, and probe failure diagnostics (#109).
	mlConfig.LogOutput = &memberlistLogWriter{}
	mlConfig.Delegate = m
	mlConfig.Events = m

	if config.NodeID != "" {
		mlConfig.Name = string(config.NodeID)
	}
	if config.BindAddr != "" {
		mlConfig.BindAddr = config.BindAddr
	} else {
		mlConfig.BindAddr = "127.0.0.1"
	}
	mlConfig.BindPort = config.BindPort
	if config.AdvertiseAddr != "" {
		mlConfig.AdvertiseAddr = config.AdvertiseAddr
	}

	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	m.memberList = ml
	m.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return ml.NumMembers() },
		RetransmitMult: 3,
	}

	return m, nil
}

// Broadcast serializes and spreads a message across the cluster using epidemic gossip.
func (m *Mesh) Broadcast(msg []byte) {
	m.broadcasts.QueueBroadcast(&broadcast{
		msg: msg,
	})
}

// Members returns the gRPC API addresses of all active peers discovered via gossip.
func (m *Mesh) Members() []PeerAddress {
	if m.stopping.Load() || m.memberList == nil {
		return nil
	}
	var addrs []PeerAddress
	m.nodeAddrs.Range(func(_, value any) bool {
		addrs = append(addrs, value.(PeerAddress))
		return true
	})
	return addrs
}

// AddressForNode returns the gRPC address for a given node ID.
func (m *Mesh) AddressForNode(nodeID kv.NodeID) PeerAddress {
	if m.stopping.Load() || m.memberList == nil {
		return ""
	}
	val, ok := m.nodeAddrs.Load(nodeID)
	if !ok {
		return ""
	}
	return val.(PeerAddress)
}

// Owner returns the NodeID of the peer responsible for the given key.
func (m *Mesh) Owner(key kv.Key) kv.NodeID {
	return m.ring.GetNode(key)
}

// GetOwners returns the N closest NodeIDs on the hash ring responsible for replicating the given key.
func (m *Mesh) GetOwners(key kv.Key, n int) []kv.NodeID {
	return m.ring.GetOwners(key, n)
}

// UpdateLocalWeight updates the weight of the local node and triggers cluster-wide gossip.
func (m *Mesh) UpdateLocalWeight(weight int) {
	// #nosec G115
	m.localWeight.Store(int32(weight))
	if m.memberList != nil {
		_ = m.memberList.UpdateNode(time.Second)
	}
}

// LocalGossipPort returns the port memberlist actually bound to, resolving
// what Config.BindPort=0 (dynamic allocation) becomes at runtime (#91).
func (m *Mesh) LocalGossipPort() int {
	if m.memberList == nil {
		return 0
	}
	return int(m.memberList.LocalNode().Port)
}

// NotifyJoin is called by memberlist when a new node joins.
func (m *Mesh) NotifyJoin(node *memberlist.Node) {
	if node == nil {
		return
	}
	slog.Info("Node joined cluster", "node", node.Name, "addr", node.Addr.String())

	weight := defaultVnodes
	if len(node.Meta) > 0 {
		var meta pb.NodeMetadata
		if err := proto.Unmarshal(node.Meta, &meta); err == nil {
			if meta.GrpcPort > 0 {
				m.nodeAddrs.Store(kv.NodeID(node.Name), PeerAddress(fmt.Sprintf("%s:%d", node.Addr.String(), meta.GrpcPort)))
			}
			weight = int(meta.Weight)
		} else {
			slog.Error("Failed to unmarshal join metadata", "error", err)
		}
	}
	m.ring.AddNodeWithWeight(kv.NodeID(node.Name), weight)
}

// NotifyLeave is called by memberlist when a node leaves.
func (m *Mesh) NotifyLeave(node *memberlist.Node) {
	if node == nil {
		return
	}
	slog.Info("Node left cluster", "node", node.Name)
	m.ring.RemoveNode(kv.NodeID(node.Name))
	m.nodeAddrs.Delete(kv.NodeID(node.Name))
}

// NotifyUpdate is called by memberlist when a node's metadata changes.
func (m *Mesh) NotifyUpdate(node *memberlist.Node) {
	if node == nil {
		return
	}
	weight := defaultVnodes
	if len(node.Meta) > 0 {
		var meta pb.NodeMetadata
		if err := proto.Unmarshal(node.Meta, &meta); err == nil {
			if meta.GrpcPort > 0 {
				m.nodeAddrs.Store(kv.NodeID(node.Name), PeerAddress(fmt.Sprintf("%s:%d", node.Addr.String(), meta.GrpcPort)))
			}
			weight = int(meta.Weight)
		} else {
			slog.Error("Failed to unmarshal update metadata", "error", err)
		}
	}
	m.ring.UpdateNodeWeight(kv.NodeID(node.Name), weight)
}

// Start joins the cluster, retrying Join with backoff so a peer's DNS lag
// during Kubernetes pod scheduling isn't treated as fatal (#93).
func (m *Mesh) Start() error {
	if len(m.config.SeedNodes) == 0 {
		return nil
	}

	retries := m.config.JoinRetries
	if retries <= 0 {
		retries = defaultJoinRetries
	}
	delay := m.config.JoinRetryBaseDelay
	if delay <= 0 {
		delay = defaultJoinRetryBaseDelay
	}

	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		count, err := m.memberList.Join(m.config.SeedNodes)
		if err == nil {
			slog.Info("Joined cluster successfully", "seeds", len(m.config.SeedNodes), "joined", count, "attempt", attempt)
			return nil
		}
		lastErr = err

		if attempt < retries {
			slog.Warn("Failed to join cluster, retrying", "attempt", attempt, "retries", retries, "delay", delay, "error", err)
			time.Sleep(delay)
			delay = min(delay*2, maxJoinRetryDelay)
		}
	}
	return fmt.Errorf("failed to join cluster after %d attempts: %w", retries, lastErr)
}

// Stop gracefully stops the gossip node membership service.
func (m *Mesh) Stop() error {
	m.stopping.Store(true)
	if m.memberList == nil {
		return nil
	}

	slog.Info("Leaving cluster...")
	if err := m.memberList.Leave(time.Second); err != nil {
		slog.Warn("Error during cluster leave", "error", err)
	}

	err := m.memberList.Shutdown()
	return err
}

// memberlist.Delegate implementation

// NodeMeta returns the metadata of the node, which includes the gRPC port and dynamic weight.
func (m *Mesh) NodeMeta(_ int) []byte {
	// #nosec G115
	meta := &pb.NodeMetadata{
		GrpcPort: int32(m.config.GrpcPort),
		Weight:   m.localWeight.Load(),
	}
	b, err := proto.Marshal(meta)
	if err != nil {
		slog.Error("Failed to marshal node metadata", "error", err)
		return nil
	}
	return b
}

// NotifyMsg is called when a user-space message is received.
func (m *Mesh) NotifyMsg(b []byte) {
	m.gossip.OnGossip(b)
}

// GetBroadcasts is called when memberlist needs messages to broadcast.
func (m *Mesh) GetBroadcasts(overhead, limit int) [][]byte {
	return m.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState no longer carries the database over gossip; entropy.Syncer
// handles bulk sync via gRPC instead (#63).
func (m *Mesh) LocalState(_ bool) []byte {
	return nil
}

// MergeRemoteState is a no-op for the same reason LocalState returns nil.
func (m *Mesh) MergeRemoteState(_ []byte, _ bool) {}

// NopMesh is a non-functional implementation of the Mesh interface used when distribution is disabled.
type NopMesh struct{}

// Broadcast does nothing in a NopMesh.
func (n *NopMesh) Broadcast([]byte) {
	_ = n
}

// Members returns nil as there are no members in a NopMesh.
func (n *NopMesh) Members() []PeerAddress { return nil }

// Owner returns an empty NodeID as there are no owners in a NopMesh.
func (n *NopMesh) Owner(kv.Key) kv.NodeID { return "" }

// GetOwners returns nil as there are no owners in a NopMesh.
func (n *NopMesh) GetOwners(kv.Key, int) []kv.NodeID { return nil }

// AddressForNode returns an empty PeerAddress in NopMesh.
func (n *NopMesh) AddressForNode(kv.NodeID) PeerAddress { return "" }

// Start does nothing in a NopMesh.
func (n *NopMesh) Start() error { return nil }

// Stop does nothing in a NopMesh.
func (n *NopMesh) Stop() error { return nil }

// UpdateLocalWeight does nothing in a NopMesh.
func (n *NopMesh) UpdateLocalWeight(_ int) {}

// LocalGossipPort always returns 0 in a NopMesh: nothing gossips.
func (n *NopMesh) LocalGossipPort() int { return 0 }

type broadcast struct {
	msg []byte
}

func (b *broadcast) Invalidates(_ memberlist.Broadcast) bool { return false }
func (b *broadcast) Message() []byte                         { return b.msg }
func (b *broadcast) Finished() {
	_ = b
}

// memberlistLogWriter bridges memberlist's stdlib log output to slog,
// surfacing network partition, probe failure, and node failure diagnostics
// that would otherwise be discarded (#109).
type memberlistLogWriter struct{}

func (w *memberlistLogWriter) Write(p []byte) (n int, err error) {
	msg := string(bytes.TrimRight(p, "\n"))
	slog.Debug("memberlist", "msg", msg)
	return len(p), nil
}
