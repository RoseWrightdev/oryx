package server

import (
	"context"
	"testing"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/cluster/entropy"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/core"
	"github.com/rosewrightdev/oryx/core/hashmap"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type mockDatabase struct {
	mock.Mock
}

func (m *mockDatabase) Core() core.Engine {
	return nil
}

func (m *mockDatabase) Creds() credentials.TransportCredentials {
	for _, call := range m.ExpectedCalls {
		if call.Method == "Creds" {
			args := m.Called()
			if v := args.Get(0); v != nil {
				return v.(credentials.TransportCredentials)
			}
			return nil
		}
	}
	return nil
}

func (m *mockDatabase) Get(key kv.Key) ([]byte, bool) {
	args := m.Called(key)
	if v := args.Get(0); v != nil {
		return v.([]byte), args.Bool(1)
	}
	return nil, args.Bool(1)
}

func (m *mockDatabase) Set(key kv.Key, value []byte) error {
	args := m.Called(key, value)
	return args.Error(0)
}

func (m *mockDatabase) Delete(key kv.Key) (bool, error) {
	args := m.Called(key)
	return args.Bool(0), args.Error(1)
}

func (m *mockDatabase) Owner(key kv.Key) kv.NodeID {
	args := m.Called(key)
	return kv.NodeID(args.String(0))
}

func (m *mockDatabase) NodeID() kv.NodeID {
	args := m.Called()
	return kv.NodeID(args.String(0))
}

func (m *mockDatabase) Start() { m.Called() }
func (m *mockDatabase) Stop()  { m.Called() }
func (m *mockDatabase) Addr() string {
	args := m.Called()
	return args.String(0)
}
func (m *mockDatabase) GossipAddr() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockDatabase) Mesh() mesh.Mesher {
	args := m.Called()
	return args.Get(0).(mesh.Mesher)
}

func (m *mockDatabase) Snapshot() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockDatabase) SyncPull(pullConfig *entropy.PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	args := m.Called(pullConfig.RequesterID, pullConfig.Root, pullConfig.Shards, pullConfig.Buckets)
	return args.Get(0).([]*pb.SetRequest), args.Get(1).([]*pb.DeleteRequest), args.Error(2)
}

func (m *mockDatabase) SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error {
	args := m.Called(sets, deletes)
	return args.Error(0)
}

func (m *mockDatabase) RootDigest() hashmap.RootDigest {
	return m.Called().Get(0).(hashmap.RootDigest)
}

func (m *mockDatabase) FillShardDigests(dst map[hashmap.ShardID]hashmap.Digest) {
	m.Called(dst)
}

func (m *mockDatabase) FillDigests(dst map[hashmap.ShardID]hashmap.ShardDigest) {
	m.Called(dst)
}

func TestServerHandlers(t *testing.T) {
	me := new(mockDatabase)
	srv := &server{
		eng:   me,
		pools: newServerPools(),
	}

	t.Run("Pull_Success", func(t *testing.T) {
		root := hashmap.RootDigest(12345)
		buckets := map[hashmap.ShardID]hashmap.ShardDigest{1: make([]hashmap.Digest, 64)}
		buckets[1][0] = 123

		sets := []*pb.SetRequest{{Key: "k1", Value: []byte("v1")}}
		deletes := []*pb.DeleteRequest{{Key: "k2"}}

		me.On("SyncPull", mock.Anything, root, mock.Anything, mock.Anything).Return(sets, deletes, nil).Once()

		req := &pb.PullRequest{
			RootDigest:   root,
			ShardDigests: map[uint32]uint64{1: 123},
			SubDigests:   map[uint32]*pb.ShardDigests{1: {SubHashes: buckets[1]}},
		}
		resp, err := srv.Pull(context.Background(), req)
		assert.NoError(t, err)
		assert.Equal(t, sets, resp.Entries)
		assert.Equal(t, deletes, resp.Deletions)
		me.AssertExpectations(t)
	})

	t.Run("Push_Success", func(t *testing.T) {
		sets := []*pb.SetRequest{{Key: "k1"}}
		deletes := []*pb.DeleteRequest{{Key: "k2"}}

		me.On("SyncPush", sets, deletes).Return(nil).Once()

		_, err := srv.Push(context.Background(), &pb.PushRequest{Entries: sets, Deletions: deletes})
		assert.NoError(t, err)
		me.AssertExpectations(t)
	})

	t.Run("Pull_Error", func(t *testing.T) {
		me.On("SyncPull", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*pb.SetRequest{}, []*pb.DeleteRequest{}, assert.AnError).Once()

		_, err := srv.Pull(context.Background(), &pb.PullRequest{})
		assert.Error(t, err)
		me.AssertExpectations(t)
	})

	t.Run("Push_Error", func(t *testing.T) {
		me.On("SyncPush", mock.Anything, mock.Anything).Return(assert.AnError).Once()

		_, err := srv.Push(context.Background(), &pb.PushRequest{})
		assert.Error(t, err)
		me.AssertExpectations(t)
	})
}

func TestServer_PoolDegradation(t *testing.T) {
	me := new(mockDatabase)
	srv := &server{
		eng:   me,
		pools: newServerPools(),
	}

	req1 := &pb.PullRequest{
		RootDigest: 12345,
		SubDigests: map[uint32]*pb.ShardDigests{
			1: {SubHashes: make([]hashmap.Digest, 64)},
		},
	}
	me.On("SyncPull", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*pb.SetRequest{}, []*pb.DeleteRequest{}, nil).Once()
	_, err := srv.Pull(context.Background(), req1)
	assert.NoError(t, err)

	var capturedBuckets map[hashmap.ShardID]hashmap.ShardDigest
	me.On("SyncPull", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		capturedBuckets = args.Get(3).(map[hashmap.ShardID]hashmap.ShardDigest)
	}).Return([]*pb.SetRequest{}, []*pb.DeleteRequest{}, nil).Once()

	_, err = srv.Pull(context.Background(), &pb.PullRequest{})
	assert.NoError(t, err)

	assert.Equal(t, 128, len(capturedBuckets), "The recycled buckets map should contain all 128 pre-allocated shard entries!")
}

func TestServer_ExtraEdgeCases(t *testing.T) {
	me := new(mockDatabase)
	srv := &server{
		eng:   me,
		pools: newServerPools(),
	}

	// 1. Get handler
	me.On("Get", kv.Key("k-get")).Return([]byte("v-get"), true).Once()
	respGet, err := srv.Get(context.Background(), &pb.GetRequest{Key: "k-get"})
	assert.NoError(t, err)
	assert.True(t, respGet.Exists)
	assert.Equal(t, []byte("v-get"), respGet.Value)

	// 2. Set error
	me.On("Set", kv.Key("k-set"), []byte("v-set")).Return(assert.AnError).Once()
	_, err = srv.Set(context.Background(), &pb.SetRequest{Key: "k-set", Value: []byte("v-set")})
	assert.Error(t, err)

	// Set success
	me.On("Set", kv.Key("k-set"), []byte("v-set")).Return(nil).Once()
	_, err = srv.Set(context.Background(), &pb.SetRequest{Key: "k-set", Value: []byte("v-set")})
	assert.NoError(t, err)

	// 3. Delete error
	me.On("Delete", kv.Key("k-del")).Return(false, assert.AnError).Once()
	_, err = srv.Delete(context.Background(), &pb.DeleteRequest{Key: "k-del"})
	assert.Error(t, err)

	// Delete success
	me.On("Delete", kv.Key("k-del")).Return(true, nil).Once()
	_, err = srv.Delete(context.Background(), &pb.DeleteRequest{Key: "k-del"})
	assert.NoError(t, err)

	// 4. Grpc.Run failure
	me.On("Addr").Return("invalid-address-format-abc").Once()
	gServer := NewServer(me)
	err = gServer.Run()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create listener on")

	// 5. Grpc life cycle (Mocking Stop calls)
	me.On("Stop").Twice()
	gServer.Stop()
	gServer.HardStop()

	me.AssertExpectations(t)
}

func TestServer_WithCredentials(t *testing.T) {
	me := new(mockDatabase)
	me.On("Creds").Return(insecure.NewCredentials()).Once()
	srv := NewServer(me)
	assert.NotNil(t, srv)
	me.AssertExpectations(t)
}
