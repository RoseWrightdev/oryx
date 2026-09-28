// Package mesh provides decentralized cluster discovery, gossip membership, and key routing.
package mesh

import (
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/security"
)

const (
	defaultVnodes = 128
)

// HashRing implements consistent hashing for data partitioning across oryx nodes.
type HashRing struct {
	nodes    map[kv.NodeID]uint32
	weights  map[kv.NodeID]int
	nodeList []kv.NodeID
	vnodes   []vnode
	mu       sync.RWMutex
}

// Splitting a physical node into virtual positions avoids statistical hotspotting
// and ensures a uniform keyspace distribution across the cluster.
type vnode struct {
	hash    uint64
	nodeIdx uint32
}

var vnodeSlicePool = sync.Pool{
	New: func() any {
		s := make([]vnode, 0, 1024)
		return &s
	},
}

// NewHashRing initializes an empty consistent hashing ring.
func NewHashRing() *HashRing {
	return &HashRing{
		nodes:   make(map[kv.NodeID]uint32),
		weights: make(map[kv.NodeID]int),
	}
}

// AddNode inserts a node into the ring with a predefined number of virtual nodes.
func (r *HashRing) AddNode(nodeID kv.NodeID) {
	r.AddNodeWithWeight(nodeID, defaultVnodes)
}

// AddNodeWithWeight inserts a node into the ring with a specific dynamic weight.
func (r *HashRing) AddNodeWithWeight(nodeID kv.NodeID, weight int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; exists {
		r.weights[nodeID] = weight
		r.rebuildVnodes()
		return
	}

	// #nosec G115
	nodeIdx := uint32(len(r.nodeList))
	r.nodes[nodeID] = nodeIdx
	r.nodeList = append(r.nodeList, nodeID)
	r.weights[nodeID] = weight

	r.rebuildVnodes()
}

// UpdateNodeWeight updates the dynamic weight (number of virtual nodes) for a node.
func (r *HashRing) UpdateNodeWeight(nodeID kv.NodeID, weight int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; !exists {
		return
	}
	r.weights[nodeID] = weight
	r.rebuildVnodes()
}

func (r *HashRing) rebuildVnodes() {
	totalVnodes := 0
	for _, id := range r.nodeList {
		w, ok := r.weights[id]
		if !ok || w < 0 {
			w = defaultVnodes
		}
		totalVnodes += w
	}

	// Lease scratch space for the new ring. An undersized lease is dropped for
	// the GC rather than returned to the pool: putting it back would leave the
	// pool holding slices that can never satisfy a grown ring, so every later
	// Get would hand one out, fail the capacity check, and allocate anyway.
	var newVnodes []vnode
	if sPtr, ok := vnodeSlicePool.Get().(*[]vnode); ok && cap(*sPtr) >= totalVnodes {
		newVnodes = (*sPtr)[:totalVnodes]
	} else {
		newVnodes = make([]vnode, totalVnodes)
	}

	vIdx := 0
	for _, id := range r.nodeList {
		w, ok := r.weights[id]
		if !ok || w < 0 {
			w = defaultVnodes
		}
		nodeIdx := r.nodes[id]
		for j := range w {
			hash := security.HashFuncSecure(fmt.Sprintf("%s-%d", id, j))
			newVnodes[vIdx] = vnode{hash: hash, nodeIdx: nodeIdx}
			vIdx++
		}
	}

	slices.SortFunc(newVnodes, func(a, b vnode) int {
		if a.hash < b.hash {
			return -1
		} else if a.hash > b.hash {
			return 1
		}
		return 0
	})

	oldVnodes := r.vnodes
	r.vnodes = newVnodes

	// Recycle the ring we just retired, never the one we just published.
	// Readers only touch r.vnodes under the read lock and this runs under the
	// write lock, so the retired array has no live observers once we return;
	// newVnodes, by contrast, is the live ring and must never enter the pool.
	// Exactly one slice goes in per rebuild, so the pool cannot accumulate.
	if cap(oldVnodes) > 0 {
		recycled := oldVnodes[:0]
		vnodeSlicePool.Put(&recycled)
	}
}

// AddNodes inserts multiple nodes into the ring in a single highly-optimized pass.
func (r *HashRing) AddNodes(nodeIDs []kv.NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var newNodes []kv.NodeID
	for _, id := range nodeIDs {
		if _, exists := r.nodes[id]; !exists {
			newNodes = append(newNodes, id)
		}
	}

	if len(newNodes) == 0 {
		return
	}

	startIdx := len(r.nodeList)
	for i, id := range newNodes {
		// #nosec G115
		r.nodes[id] = uint32(startIdx + i)
		r.nodeList = append(r.nodeList, id)
		r.weights[id] = defaultVnodes
	}

	r.rebuildVnodes()
}

// RemoveNode removes a node and its virtual nodes from the ring.
func (r *HashRing) RemoveNode(nodeID kv.NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, exists := r.nodes[nodeID]
	if !exists {
		return
	}

	newNodeList := make([]kv.NodeID, 0, len(r.nodeList)-1)
	newNodes := make(map[kv.NodeID]uint32)

	idx := uint32(0)
	for _, id := range r.nodeList {
		if id != nodeID {
			newNodeList = append(newNodeList, id)
			newNodes[id] = idx
			idx++
		}
	}

	r.nodeList = newNodeList
	r.nodes = newNodes
	delete(r.weights, nodeID)

	r.rebuildVnodes()
}

// GetNode returns the ID of the node responsible for the given key.
func (r *HashRing) GetNode(key kv.Key) kv.NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return ""
	}

	hash := security.HashFuncSecure(key)

	// Perform an O(log K) binary search to locate the clockwise neighbor
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= hash
	})

	// Circular Wrap-around: If the key hash is greater than all virtual node hashes on the ring,
	// wrap back geometrically to index 0 (the first virtual node on the circle).
	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.nodeList[r.vnodes[idx].nodeIdx]
}

// GetOwners returns the N nodes responsible for a key in clockwise order.
func (r *HashRing) GetOwners(key kv.Key, replicationFactor int) []kv.NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	// The ring can hold fewer physical nodes than the configured replication
	// factor — routinely so during startup, when a 2-node cluster is still
	// forming under replicationFactor=3. Without this cap the walk below can
	// never reach replicationFactor and instead scans every virtual node on the
	// ring, running thousands of slices.Contains checks on every read and write.
	target := replicationFactor
	if target > len(r.nodeList) {
		target = len(r.nodeList)
	}
	if target <= 0 {
		return nil
	}

	hash := security.HashFuncSecure(key)

	// Locate the starting index clockwise from the key's hash with binary search
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= hash
	})

	owners := make([]kv.NodeID, 0, target)

	// Walk the circle clockwise modulo the ring length to gather N distinct physical nodes
	for i := 0; i < len(r.vnodes) && len(owners) < target; i++ {
		vnodeIdx := (idx + i) % len(r.vnodes)
		node := r.nodeList[r.vnodes[vnodeIdx].nodeIdx]

		// Skip this vnode if its physical host is already in the owner list.
		// This ensures replicas are placed on separate physical nodes for fault tolerance.
		duplicate := slices.Contains(owners, node)
		if !duplicate {
			owners = append(owners, node)
		}
	}

	return owners
}


// GetNodes returns all unique node IDs currently in the ring.
func (r *HashRing) GetNodes() []kv.NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]kv.NodeID, 0, len(r.nodes))
	for node := range r.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}
