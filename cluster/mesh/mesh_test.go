package mesh

import (
	"sync"
	"testing"
	"time"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type mockGossip struct{}

func newMockGossip() *mockGossip         { return &mockGossip{} }
func (mg *mockGossip) OnGossip(_ []byte) {}

func TestClusterMembership(t *testing.T) {
	// Start first node
	c1 := Config{
		NodeID:   "node1",
		BindPort: 7001,
		GrpcPort: 8001,
	}
	mg1 := newMockGossip()
	s1, err := NewMesh(mg1, c1)
	require.NoError(t, err)
	defer func() {
		_ = s1.Stop()
	}()

	// Start second node and join first
	c2 := Config{
		NodeID:    "node2",
		BindPort:  7002,
		SeedNodes: []string{"127.0.0.1:7001"},
		GrpcPort:  8002,
	}
	mg2 := newMockGossip()
	s2, err := NewMesh(mg2, c2)
	require.NoError(t, err)
	defer func() {
		_ = s2.Stop()
	}()

	err = s2.Start()
	require.NoError(t, err)

	// Wait for convergence
	time.Sleep(200 * time.Millisecond)

	members := s1.Members()
	assert.GreaterOrEqual(t, len(members), 2)

	hasPort := func(list []PeerAddress, port string) bool {
		for _, m := range list {
			s := string(m)
			if len(s) >= len(port) && s[len(s)-len(port):] == port {
				return true
			}
		}
		return false
	}

	assert.True(t, hasPort(members, ":8001"), "Members should contain node on gRPC port 8001")
	assert.True(t, hasPort(members, ":8002"), "Members should contain node on gRPC port 8002")
}

func TestMesher_ConcurrentStop(t *testing.T) {
	c1 := Config{
		NodeID:   "node1",
		BindPort: 7003,
		GrpcPort: 8003,
	}

	mg := newMockGossip()
	s1, err := NewMesh(mg, c1)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range 50 {
			_ = s1.Members()
			_ = s1.AddressForNode("node1")
			time.Sleep(1 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = s1.Stop()
	}()

	wg.Wait()
}

type trackingGossiper struct {
	received []byte
}

func (tg *trackingGossiper) OnGossip(b []byte) {
	tg.received = b
}

func TestMesh_DelegateCallbacks(t *testing.T) {
	cfg := Config{
		NodeID:   "test-delegate-node",
		BindPort: 7005,
		GrpcPort: 8005,
	}
	gs := &trackingGossiper{}

	m, err := NewMesh(gs, cfg)
	require.NoError(t, err)
	defer func() {
		_ = m.Stop()
	}()

	// 1. NodeMeta
	metaBytes := m.NodeMeta(0)
	var meta pb.NodeMetadata
	err = proto.Unmarshal(metaBytes, &meta)
	require.NoError(t, err)
	assert.Equal(t, int32(8005), meta.GrpcPort)
	assert.Equal(t, int32(128), meta.Weight)

	// 2. NotifyMsg
	m.NotifyMsg([]byte("gossip-payload"))
	assert.Equal(t, []byte("gossip-payload"), gs.received)

	// 3. LocalState no longer carries the database (#63): it must stay nil so
	// memberlist's push/pull gossip never serializes the full key space.
	assert.Nil(t, m.LocalState(false))

	// 4. MergeRemoteState is a no-op for the same reason; it must not panic.
	assert.NotPanics(t, func() { m.MergeRemoteState([]byte("incoming-state"), false) })

	// 5. NotifyUpdate
	m.NotifyUpdate(nil) // should be no-op

	// 6. Broadcast logic
	m.Broadcast([]byte("broadcast-msg"))
	bcList := m.GetBroadcasts(0, 100)
	assert.NotEmpty(t, bcList)

	// Cover broadcast struct methods
	bcStruct := &broadcast{msg: []byte("bc")}
	assert.False(t, bcStruct.Invalidates(nil))
	assert.Equal(t, []byte("bc"), bcStruct.Message())
	bcStruct.Finished()
}

func TestNopMesh(t *testing.T) {
	n := &NopMesh{}
	n.Broadcast([]byte("noop"))
	assert.Nil(t, n.Members())
	assert.Equal(t, kv.NodeID(""), n.Owner(kv.Key("key")))
	assert.Nil(t, n.GetOwners(kv.Key("key"), 3))
	assert.Equal(t, PeerAddress(""), n.AddressForNode(kv.NodeID("node")))
	assert.NoError(t, n.Start())
	assert.NoError(t, n.Stop())
	assert.Equal(t, 0, n.LocalGossipPort())
}

// TestMesh_LocalGossipPortResolvesDynamicPort pins #91: BindPort=0 must
// resolve to the real bound port, not report 0.
func TestMesh_LocalGossipPortResolvesDynamicPort(t *testing.T) {
	mg := newMockGossip()
	m, err := NewMesh(mg, Config{NodeID: "dynamic-port-node", BindPort: 0})
	require.NoError(t, err)
	defer func() { _ = m.Stop() }()

	assert.Positive(t, m.LocalGossipPort(), "a real port must be resolved even though BindPort was 0")

	// A Mesh not yet backed by a memberlist instance reports 0 rather than panicking.
	assert.Zero(t, (&Mesh{}).LocalGossipPort())
}

func TestMesh_ExtraEdgeCases(t *testing.T) {
	// 1. NewMesh with invalid config to force failure
	cfg := Config{
		BindAddr: "invalid-ip-address!!!",
		BindPort: -100,
	}
	mg := newMockGossip()
	m, err := NewMesh(mg, cfg)
	assert.Error(t, err)
	assert.Nil(t, m)

	// 2. stop with nil memberList
	mNil := &Mesh{}
	assert.NoError(t, mNil.Stop())

	// 3. start join failure. JoinRetries pinned to 1: retry/backoff is
	// covered separately in TestMesh_StartRetriesJoinOnFailure.
	cfgJoin := Config{
		NodeID:      "test-join-fail",
		BindPort:    7099,
		SeedNodes:   []string{"0.0.0.0:0"}, // invalid / unreachable seed
		JoinRetries: 1,
	}
	mJoin, err := NewMesh(mg, cfgJoin)
	require.NoError(t, err)
	defer func() {
		_ = mJoin.Stop()
	}()
	err = mJoin.Start()
	assert.Error(t, err)

	// 4. AddressForNode when stopped or nil memberList
	assert.Empty(t, mNil.AddressForNode("some-node"))
	assert.Empty(t, mNil.Members())
	assert.Nil(t, mNil.LocalState(false))
}

// TestMesh_StartRetriesJoinOnFailure covers #93: a peer's DNS record not
// yet published when this node starts joining must not be treated as fatal.
func TestMesh_StartRetriesJoinOnFailure(t *testing.T) {
	mg := newMockGossip()

	t.Run("succeeds once the seed becomes reachable mid-retry", func(t *testing.T) {
		joiner, err := NewMesh(mg, Config{
			NodeID:             "retry-joiner",
			BindPort:           7101,
			SeedNodes:          []string{"127.0.0.1:7100"},
			JoinRetries:        6,
			JoinRetryBaseDelay: 40 * time.Millisecond,
		})
		require.NoError(t, err)
		defer func() { _ = joiner.Stop() }()

		startErr := make(chan error, 1)
		go func() { startErr <- joiner.Start() }()

		// Let a couple of retries fail against the not-yet-listening seed
		// before it comes up, mirroring a peer pod scheduled a beat late.
		time.Sleep(90 * time.Millisecond)

		seed, err := NewMesh(mg, Config{NodeID: "retry-seed", BindPort: 7100})
		require.NoError(t, err)
		defer func() { _ = seed.Stop() }()

		select {
		case err := <-startErr:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Start did not return after the seed became reachable")
		}
	})

	t.Run("gives up after exhausting retries against an unreachable seed", func(t *testing.T) {
		m, err := NewMesh(mg, Config{
			NodeID:             "retry-exhausted",
			BindPort:           7102,
			SeedNodes:          []string{"0.0.0.0:0"},
			JoinRetries:        3,
			JoinRetryBaseDelay: 10 * time.Millisecond,
		})
		require.NoError(t, err)
		defer func() { _ = m.Stop() }()

		err = m.Start()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "after 3 attempts")
	})
}
