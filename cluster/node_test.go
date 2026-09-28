package cluster

import (
	"testing"

	"github.com/rosewrightdev/oryx/cluster/gateway"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/core/clock"
	"github.com/rosewrightdev/oryx/core/evict"
	"github.com/rosewrightdev/oryx/core/hashmap"
	"github.com/rosewrightdev/oryx/core/snap"
	"github.com/rosewrightdev/oryx/core/wal"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeEvictor records Publish calls so tests can assert eviction telemetry
// fired (or didn't) without needing a real LRU.
type fakeEvictor struct {
	published []kv.Key
}

func (f *fakeEvictor) Publish(key kv.Key, _ kv.HashKey)                  { f.published = append(f.published, key) }
func (f *fakeEvictor) PublishDelete(kv.Key, kv.HashKey)                  {}
func (f *fakeEvictor) Start()                                            {}
func (f *fakeEvictor) Stop()                                             {}
func (f *fakeEvictor) SetEvictCallback(func(kv.Key, evict.Reason) error) {}

// fakeEngine implements core.Engine with just enough behavior for Node.Get:
// a real ShardedMap backing HM() and an observable Evt().
type fakeEngine struct {
	hm  *hashmap.ShardedMap
	evt *fakeEvictor
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{
		hm:  hashmap.NewShardedMap(),
		evt: &fakeEvictor{},
	}
}

func (f *fakeEngine) Get(key kv.Key) ([]byte, bool) {
	val, ok := f.hm.Get(key)
	if ok && f.evt != nil {
		f.evt.Publish(key, 0)
	}
	return val, ok
}
func (f *fakeEngine) Set(kv.Key, []byte) error         { return nil }
func (f *fakeEngine) Put(kv.Key, kv.Value) error       { return nil }
func (f *fakeEngine) Delete(kv.Key) (bool, error)      { return false, nil }
func (f *fakeEngine) Exists(kv.Key) bool               { return false }
func (f *fakeEngine) Start()                           {}
func (f *fakeEngine) Stop()                            {}
func (f *fakeEngine) HM() *hashmap.ShardedMap          { return f.hm }
func (f *fakeEngine) Wal() wal.Waler                   { return nil }
func (f *fakeEngine) Clock() clock.Clocker             { return nil }
func (f *fakeEngine) Snp() *snap.Snapshotter           { return nil }
func (f *fakeEngine) Evt() evict.Evictor               { return f.evt }
func (f *fakeEngine) Evict(kv.Key, evict.Reason) error { return nil }
func (f *fakeEngine) Occupancy() float64               { return 0 }

// nodeMockMesher is a minimal mesh.Mesher whose owner set is directly
// controllable, to exercise isOwner's true/false branches.
type nodeMockMesher struct {
	mesh.NopMesh
	owners []kv.NodeID
}

func (m *nodeMockMesher) GetOwners(kv.Key, int) []kv.NodeID { return m.owners }

func newTestNode(t *testing.T, eng *fakeEngine, meshObj mesh.Mesher, nodeID kv.NodeID) *Node {
	t.Helper()
	cfg := mesh.Config{NodeID: nodeID, ReplicationFactor: 1, ReplicationFailureMode: mesh.Lenient}
	return &Node{
		core:       eng,
		mesh:       meshObj,
		meshConfig: cfg,
		gw:         gateway.NewGateway(meshObj, &cfg, insecure.NewCredentials()),
	}
}

// TestNode_Get_SingleNodePublishesEvictionTelemetry pins #84: LoadData's
// fast path must still report reads to the evictor, or hot keys get evicted.
func TestNode_Get_SingleNodePublishesEvictionTelemetry(t *testing.T) {
	eng := newFakeEngine()
	key := kv.Key("hot-key")
	eng.hm.Put(key, kv.Value{Data: []byte("val")})

	n := newTestNode(t, eng, &mesh.NopMesh{}, "solo")
	n.meshConfig.SingleNode = true

	data, ok := n.Get(key)
	assert.True(t, ok)
	assert.Equal(t, []byte("val"), data)
	assert.Equal(t, []kv.Key{key}, eng.evt.published, "a successful read must publish eviction telemetry")

	_, ok = n.Get("missing-key")
	assert.False(t, ok)
	assert.Len(t, eng.evt.published, 1, "a miss must not publish telemetry")
}

// TestNode_Get_DistributedServesLocalWhenOwner covers the common fast path:
// a current owner with the key locally must not proxy or lose telemetry.
func TestNode_Get_DistributedServesLocalWhenOwner(t *testing.T) {
	eng := newFakeEngine()
	key := kv.Key("owned-key")
	eng.hm.Put(key, kv.Value{Data: []byte("val")})

	n := newTestNode(t, eng, &nodeMockMesher{owners: []kv.NodeID{"self"}}, "self")

	data, ok := n.Get(key)
	assert.True(t, ok)
	assert.Equal(t, []byte("val"), data)
	assert.Equal(t, []kv.Key{key}, eng.evt.published)
}

// TestNode_Get_DistributedSkipsStaleLocalCopyWhenNotOwner pins #61: a stale
// local copy after rebalance must not shadow the real owners.
func TestNode_Get_DistributedSkipsStaleLocalCopyWhenNotOwner(t *testing.T) {
	eng := newFakeEngine()
	key := kv.Key("rebalanced-away-key")
	eng.hm.Put(key, kv.Value{Data: []byte("stale-local-value")})

	// "other" now owns this key; "self" is no longer in the owner set.
	n := newTestNode(t, eng, &nodeMockMesher{owners: []kv.NodeID{"other"}}, "self")

	_, ok := n.Get(key)
	assert.False(t, ok, "a non-owner must not serve its stale local copy")
	assert.Empty(t, eng.evt.published, "a skipped local read must not publish telemetry for a stale copy")
}

// TestNode_Get_DistributedTombstoneOnlyAuthoritativeWhenOwner extends #61 to
// tombstones: a stale local delete must not shadow the real owners either.
func TestNode_Get_DistributedTombstoneOnlyAuthoritativeWhenOwner(t *testing.T) {
	eng := newFakeEngine()
	key := kv.Key("stale-tombstone-key")
	eng.hm.Put(key, kv.Value{Tombstone: true})

	nOwner := newTestNode(t, eng, &nodeMockMesher{owners: []kv.NodeID{"self"}}, "self")
	_, ok := nOwner.Get(key)
	assert.False(t, ok, "an owner's local tombstone is authoritative")

	nNotOwner := newTestNode(t, eng, &nodeMockMesher{owners: []kv.NodeID{"other"}}, "self")
	_, ok = nNotOwner.Get(key)
	assert.False(t, ok, "falls through to proxy, which also finds nothing here")
}

// BenchmarkNode_Get_DistributedOwner measures the added isOwner cost (#61)
// on the common fast path: an owner reading a key it holds locally.
func BenchmarkNode_Get_DistributedOwner(b *testing.B) {
	eng := newFakeEngine()
	key := kv.Key("bench-key")
	eng.hm.Put(key, kv.Value{Data: []byte("val")})

	n := &Node{
		core:       eng,
		mesh:       &nodeMockMesher{owners: []kv.NodeID{"self"}},
		meshConfig: mesh.Config{NodeID: "self", ReplicationFactor: 1},
	}

	b.ResetTimer()
	for b.Loop() {
		_, _ = n.Get(key)
	}
}
