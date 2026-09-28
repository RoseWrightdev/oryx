// Package cluster provides cluster discovery, consistent hash ring routing, anti-entropy synchronization, and gossip.
package cluster

import (
	"fmt"
	"slices"
	"sync"
	"time"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/cluster/entropy"
	"github.com/rosewrightdev/oryx/cluster/gateway"
	"github.com/rosewrightdev/oryx/cluster/gossip"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/cluster/stats"
	"github.com/rosewrightdev/oryx/core"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/security"
	"google.golang.org/grpc/credentials"
)

// Node wraps a core.Engine with cluster management, peer proxying, and gossip synchronization.
type Node struct {
	core       core.Engine
	mesh       mesh.Mesher
	meshConfig mesh.Config
	gw         *gateway.Gateway
	syncer     *entropy.Syncer
	monitor    *stats.Monitor
	creds      credentials.TransportCredentials
	startOnce  sync.Once
	stopOnce   sync.Once
}

// Config specifies parameters required for the cluster node.
type Config struct {
	MeshConfig     mesh.Config
	Creds          credentials.TransportCredentials
	GossipInterval time.Duration
}

// NewNode initializes a new cluster Node wrapping the given core.Engine.
func NewNode(coreEngine core.Engine, config Config) (*Node, error) {
	node := &Node{
		core:       coreEngine,
		meshConfig: config.MeshConfig,
		creds:      config.Creds,
	}

	stateWriter := NewStorageStateWriter(coreEngine)
	gossipService := gossip.NewGossip(stateWriter)

	node.mesh = &mesh.NopMesh{}
	if !config.MeshConfig.SingleNode {
		meshObj, err := mesh.NewMesh(
			gossipService,
			config.MeshConfig,
		)
		if err != nil {
			return nil, err
		}
		node.mesh = meshObj
	}

	node.gw = gateway.NewGateway(node.mesh, &node.meshConfig, config.Creds)
	node.gw.SetStateWriter(stateWriter)

	if !config.MeshConfig.SingleNode {
		node.syncer = entropy.NewSyncer(&entropy.SyncerConfig{
			NodeID:     config.MeshConfig.NodeID,
			Writer:     stateWriter,
			Mesh:       node.mesh,
			MeshConfig: &node.meshConfig,
			Hm:         coreEngine.HM(),
			Interval:   config.GossipInterval,
			Creds:      config.Creds,
			Cc:         node.gw.GetClientCache(),
		})
	}

	node.monitor = stats.NewMonitor(coreEngine.Occupancy, func(w int) {
		node.mesh.UpdateLocalWeight(w)
	})

	return node, nil
}

// Start launches the core engine and cluster background services.
func (n *Node) Start() {
	n.startOnce.Do(func() {
		n.core.Start()
		if err := n.mesh.Start(); err != nil {
			panic(fmt.Sprintf("failed to start cluster service: %v", err))
		}
		if n.syncer != nil {
			n.syncer.Start()
		}
		if n.monitor != nil {
			n.monitor.Start()
		}
	})
}

// Stop gracefully shuts down cluster background services and the core engine.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		if n.monitor != nil {
			n.monitor.Stop()
		}
		if n.syncer != nil {
			n.syncer.Stop()
		}
		if err := n.mesh.Stop(); err != nil {
			panic(fmt.Sprintf("failed to stop cluster service: %v", err))
		}
		if n.gw != nil {
			n.gw.Close()
		}
		n.core.Stop()
	})
}

// Get retrieves the value for a key locally, respects local tombstones, or proxies to an owner node.
func (n *Node) Get(key kv.Key) ([]byte, bool) {
	hash := kv.HashKey(security.HashFunc(key))
	if n.meshConfig.SingleNode {
		data, ok := n.core.HM().Get(key)
		if ok && n.core.Evt() != nil {
			n.core.Evt().Publish(key, hash)
		}
		return data, ok
	}

	// Only trust local storage while still a current owner: a stale local
	// copy from before a rebalance must not shadow the real owners (#61).
	if n.isOwner(key) {
		iv, ok := n.core.HM().GetRecord(key)
		if ok && !iv.Tombstone {
			if n.core.Evt() != nil {
				n.core.Evt().Publish(key, hash)
			}
			return iv.Data, true
		} else if ok && iv.Tombstone {
			// Local tombstone exists; key is known to be deleted
			return nil, false
		}
	}

	return n.gw.Get(key)
}

// isOwner reports whether this node is currently a replica owner for key.
func (n *Node) isOwner(key kv.Key) bool {
	rf := n.meshConfig.ReplicationFactor
	if rf <= 0 {
		rf = 1
	}
	owners := n.mesh.GetOwners(key, rf)
	defer n.mesh.PutOwners(owners)
	return slices.Contains(owners, n.meshConfig.NodeID)
}

// Set writes a key-value pair locally or forwards through the gateway to replica nodes.
func (n *Node) Set(key kv.Key, value []byte) error {
	if n.meshConfig.SingleNode {
		return n.core.Set(key, value)
	}
	return n.gw.Set(key, value, n.core.Clock().Now())
}

// Delete marks a key as deleted locally or forwards through the gateway to replica nodes.
func (n *Node) Delete(key kv.Key) (bool, error) {
	if n.meshConfig.SingleNode {
		return n.core.Delete(key)
	}
	return n.gw.Delete(key, n.core.Clock().Now())
}

// Core returns the underlying core.Engine.
func (n *Node) Core() core.Engine {
	return n.core
}

// SyncPull handles incoming anti-entropy pull requests.
func (n *Node) SyncPull(pullConfig *entropy.PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	if n.syncer == nil {
		return nil, nil, nil
	}
	return n.syncer.Pull(pullConfig)
}

// SyncPush handles incoming anti-entropy push requests.
func (n *Node) SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	if n.syncer == nil {
		return nil
	}
	return n.syncer.Push(sets, deletes)
}

// Owner returns the NodeID responsible for a given key on the hash ring.
func (n *Node) Owner(key kv.Key) kv.NodeID {
	if n.meshConfig.SingleNode {
		return n.meshConfig.NodeID
	}
	return n.mesh.Owner(key)
}

// NodeID returns the cluster node ID.
func (n *Node) NodeID() kv.NodeID {
	return n.meshConfig.NodeID
}

// Addr returns the formatted gRPC bind address.
func (n *Node) Addr() string {
	addr := n.meshConfig.BindAddr
	if addr == "" {
		panic("oryx: bind address not configured")
	}
	return fmt.Sprintf("%s:%d", addr, n.meshConfig.GrpcPort)
}

// GossipAddr returns the formatted gossip bind address, using the actual
// bound port rather than the configured one when BindPort was 0 (#91).
func (n *Node) GossipAddr() string {
	addr := n.meshConfig.BindAddr
	if addr == "" {
		panic("oryx: bind address not configured")
	}
	port := n.meshConfig.BindPort
	if actual := n.mesh.LocalGossipPort(); actual > 0 {
		port = actual
	}
	return fmt.Sprintf("%s:%d", addr, port)
}

// Mesh returns the underlying cluster Mesher.
func (n *Node) Mesh() mesh.Mesher {
	return n.mesh
}

// Creds returns the transport credentials configured on the node.
func (n *Node) Creds() credentials.TransportCredentials {
	return n.creds
}
