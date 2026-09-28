package gateway

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/kv"
	"google.golang.org/grpc/credentials"
)

// StateWriter defines the interface for applying sets and deletes to the state.
type StateWriter interface {
	ApplySet(req *pb.SetRequest) error
	ApplyDelete(req *pb.DeleteRequest) error
}

// existenceChecker is optionally implemented by a StateWriter that can
// report whether a key currently holds a live value in local storage. It lets
// the gateway answer DEL with an accurate "did it exist" result without paying
// for a remote round trip when this node is one of the key's owners.
type existenceChecker interface {
	Exists(key kv.Key) bool
}

// Gateway wraps a client cache and consistent hashing routing to proxy
// requests to the appropriate peer nodes.
type Gateway struct {
	cc         *ClientCache
	mesh       mesh.Mesher
	meshConfig *mesh.Config

	// swMu guards sw, which is installed after construction once the engine
	// finishes bootstrapping. Requests can arrive on the RESP/gRPC listeners
	// before that happens, so every read goes through stateWriter().
	swMu sync.RWMutex
	sw   StateWriter

	setRequests    sync.Pool
	deleteRequests sync.Pool
}

// NewGateway initializes a new Gateway instance.
func NewGateway(meshObj mesh.Mesher, meshConfig *mesh.Config, creds credentials.TransportCredentials) *Gateway {
	return &Gateway{
		cc:         NewClientCache(creds),
		mesh:       meshObj,
		meshConfig: meshConfig,
		setRequests: sync.Pool{
			New: func() any { return &pb.SetRequest{} },
		},
		deleteRequests: sync.Pool{
			New: func() any { return &pb.DeleteRequest{} },
		},
	}
}

// SetStateWriter registers the local state writer for processing local replicas.
func (g *Gateway) SetStateWriter(sw StateWriter) {
	g.swMu.Lock()
	g.sw = sw
	g.swMu.Unlock()
}

// stateWriter returns the registered local state writer, or an error if the
// engine has not finished bootstrapping yet. Callers must not dereference the
// result without checking the error: before SetStateWriter runs, sw is nil and
// calling through it would panic.
func (g *Gateway) stateWriter() (StateWriter, error) {
	g.swMu.RLock()
	sw := g.sw
	g.swMu.RUnlock()
	if sw == nil {
		return nil, fmt.Errorf("local state writer not registered yet")
	}
	return sw, nil
}

// Get queries the consistent hash ring for owner nodes and routes
// the read request to the first reachable peer.
func (g *Gateway) Get(key kv.Key) ([]byte, bool) {
	rf := g.getReplicationFactor()
	owners := g.mesh.GetOwners(key, rf)

	for _, owner := range owners {
		if owner == g.meshConfig.NodeID {
			continue // We already checked local storage
		}
		if val, ok, err := g.proxyGetRemote(owner, key); err == nil && ok {
			return val, true
		}
	}
	return nil, false
}

// Set queries the hash ring for owners and executes parallel writes to replicas.
func (g *Gateway) Set(key kv.Key, value []byte, ts int64) error {
	rf := g.getReplicationFactor()
	owners := g.mesh.GetOwners(key, rf)

	if len(owners) == 0 {
		return fmt.Errorf("no replica owners found for key: %s", key)
	}

	if len(owners) == 1 {
		owner := owners[0]
		if owner == g.meshConfig.NodeID {
			return g.applySetLocal(key, value, ts)
		}
		return g.applySetRemote(owner, key, value, ts)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(owners))

	for _, owner := range owners {
		wg.Add(1)
		go func(node kv.NodeID) {
			defer wg.Done()
			var err error
			if node == g.meshConfig.NodeID {
				err = g.applySetLocal(key, value, ts)
			} else {
				err = g.applySetRemote(node, key, value, ts)
			}
			if err != nil {
				errChan <- err
			}
		}(owner)
	}

	wg.Wait()
	close(errChan)

	switch g.meshConfig.ReplicationFailureMode {
	case mesh.Lenient:
		if len(errChan) == len(owners) {
			return fmt.Errorf("direct write replication failed on all replicas: %v", <-errChan)
		}
	case mesh.Strict:
		if len(errChan) != 0 {
			return fmt.Errorf("direct write replication failed on replicas: %v", <-errChan)
		}
	default:
		return fmt.Errorf("unknown meshConfig.ReplicationFailureMode: %v", g.meshConfig.ReplicationFailureMode)
	}	
	return nil
}

// Delete queries the hash ring for owners and executes parallel deletes to replicas.
//
// The returned bool reports whether the key held a live value before the
// tombstone was written, so callers can honor the Redis DEL contract of
// returning 0 for a key that was not there.
func (g *Gateway) Delete(key kv.Key, ts int64) (bool, error) {
	rf := g.getReplicationFactor()
	owners := g.mesh.GetOwners(key, rf)

	if len(owners) == 0 {
		return false, fmt.Errorf("no replica owners found for key: %s", key)
	}

	// Probe before writing the tombstone; afterwards every replica reports the
	// key as absent regardless of whether it was ever there.
	existed := g.exists(key, owners)

	if len(owners) == 1 {
		owner := owners[0]
		if owner == g.meshConfig.NodeID {
			err := g.applyDeleteLocal(key, ts)
			return existed, err
		}
		err := g.applyDeleteRemote(owner, key, ts)
		return existed, err
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(owners))

	for _, owner := range owners {
		wg.Add(1)
		go func(node kv.NodeID) {
			defer wg.Done()
			var err error
			if node == g.meshConfig.NodeID {
				err = g.applyDeleteLocal(key, ts)
			} else {
				err = g.applyDeleteRemote(node, key, ts)
			}
			if err != nil {
				errChan <- err
			}
		}(owner)
	}

	wg.Wait()
	close(errChan)

	switch g.meshConfig.ReplicationFailureMode {
	case mesh.Lenient:
		if len(errChan) == len(owners) {
			return false, fmt.Errorf("direct delete replication failed on all replicas: %v", <-errChan)
		}
	case mesh.Strict:
		if len(errChan) != 0 {
			return false, fmt.Errorf("direct delete replication failed on replicas: %v", <-errChan)
		}
	default:
		return false, fmt.Errorf("unknown meshConfig.ReplicationFailureMode: %v", g.meshConfig.ReplicationFailureMode)
	}
	return existed, nil
}

// exists reports whether any owning replica currently holds a live value for
// key. The local replica is consulted first so that a hit on this node costs no
// network round trips; remote owners are probed only if the local answer is no.
func (g *Gateway) exists(key kv.Key, owners []kv.NodeID) bool {
	localOwner := false
	for _, owner := range owners {
		if owner == g.meshConfig.NodeID {
			localOwner = true
			break
		}
	}

	if localOwner {
		if sw, err := g.stateWriter(); err == nil {
			if ec, ok := sw.(existenceChecker); ok && ec.Exists(key) {
				return true
			}
		}
	}

	for _, owner := range owners {
		if owner == g.meshConfig.NodeID {
			continue
		}
		if _, ok, err := g.proxyGetRemote(owner, key); err == nil && ok {
			return true
		}
	}
	return false
}

// Close gracefully closes all cached gRPC connections.
func (g *Gateway) Close() {
	g.cc.Close()
}

// GetClientCache returns the gateway's ClientCache.
func (g *Gateway) GetClientCache() *ClientCache {
	return g.cc
}

// Helper methods for clean request/response proxy routing

func (g *Gateway) getReplicationFactor() int {
	rf := g.meshConfig.ReplicationFactor
	if rf <= 0 {
		return 1
	}
	return rf
}

func (g *Gateway) proxyGetRemote(node kv.NodeID, key kv.Key) ([]byte, bool, error) {
	addr := g.mesh.AddressForNode(node)
	if addr == "" {
		return nil, false, fmt.Errorf("address not found for node %s", node)
	}
	client, err := g.cc.Get(addr)
	if err != nil {
		return nil, false, err
	}
	val, ok, err := client.Get(string(key))
	return val, ok, err
}

func (g *Gateway) applySetLocal(key kv.Key, value []byte, ts int64) error {
	sw, err := g.stateWriter()
	if err != nil {
		return err
	}

	req := g.setRequests.Get().(*pb.SetRequest)
	defer g.setRequests.Put(req)
	req.Key = key
	req.Value = value
	req.Timestamp = ts
	req.NodeId = string(g.meshConfig.NodeID)

	err = sw.ApplySet(req)
	req.Reset()
	return err
}

func (g *Gateway) applySetRemote(node kv.NodeID, key kv.Key, value []byte, ts int64) error {
	addr := g.mesh.AddressForNode(node)
	if addr == "" {
		return fmt.Errorf("remote replica %s address not found", node)
	}
	client, err := g.cc.Get(addr)
	if err != nil {
		return err
	}

	req := g.setRequests.Get().(*pb.SetRequest)
	defer g.setRequests.Put(req)
	req.Key = key
	req.Value = value
	req.Timestamp = ts
	req.NodeId = string(g.meshConfig.NodeID)

	pushReq := &pb.PushRequest{
		Entries: []*pb.SetRequest{req},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.API.Push(ctx, pushReq)
	req.Reset()
	return err
}

func (g *Gateway) applyDeleteLocal(key kv.Key, ts int64) error {
	sw, err := g.stateWriter()
	if err != nil {
		return err
	}

	req := g.deleteRequests.Get().(*pb.DeleteRequest)
	defer g.deleteRequests.Put(req)
	req.Key = key
	req.Timestamp = ts
	req.NodeId = string(g.meshConfig.NodeID)

	err = sw.ApplyDelete(req)
	req.Reset()
	return err
}

func (g *Gateway) applyDeleteRemote(node kv.NodeID, key kv.Key, ts int64) error {
	addr := g.mesh.AddressForNode(node)
	if addr == "" {
		return fmt.Errorf("remote replica %s address not found", node)
	}
	client, err := g.cc.Get(addr)
	if err != nil {
		return err
	}

	req := g.deleteRequests.Get().(*pb.DeleteRequest)
	defer g.deleteRequests.Put(req)
	req.Key = key
	req.Timestamp = ts
	req.NodeId = string(g.meshConfig.NodeID)

	pushReq := &pb.PushRequest{
		Deletions: []*pb.DeleteRequest{req},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.API.Push(ctx, pushReq)
	req.Reset()
	return err
}
