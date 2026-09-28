package gateway

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MockStateWriter implements StateWriter
type MockStateWriter struct {
	SetErr    error
	DeleteErr error
}

func (m *MockStateWriter) ApplySet(_ *pb.SetRequest) error {
	return m.SetErr
}

func (m *MockStateWriter) ApplyDelete(_ *pb.DeleteRequest) error {
	return m.DeleteErr
}

// MockMesher implements Mesher
type MockMesher struct {
	AddrMap         map[kv.NodeID]mesh.PeerAddress
	Owners          []kv.NodeID
	BroadcastCalled bool
}

func (m *MockMesher) Broadcast(_ []byte) {
	m.BroadcastCalled = true
}

func (m *MockMesher) Members() []mesh.PeerAddress {
	return nil
}

func (m *MockMesher) Owner(_ kv.Key) kv.NodeID {
	if len(m.Owners) > 0 {
		return m.Owners[0]
	}
	return ""
}

func (m *MockMesher) GetOwners(_ kv.Key, _ int) []kv.NodeID {
	return m.Owners
}

func (m *MockMesher) AddressForNode(nodeID kv.NodeID) mesh.PeerAddress {
	return m.AddrMap[nodeID]
}

func (m *MockMesher) Start() error {
	return nil
}

func (m *MockMesher) Stop() error {
	return nil
}

func (m *MockMesher) UpdateLocalWeight(_ int) {}

func (m *MockMesher) LocalGossipPort() int { return 0 }

// MockGrpcServer implements pb.OryxServiceServer
type MockGrpcServer struct {
	pb.UnimplementedOryxServiceServer
	GetFunc  func(req *pb.GetRequest) (*pb.GetResponse, error)
	PushFunc func(req *pb.PushRequest) (*pb.PushResponse, error)
}

func (m *MockGrpcServer) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if m.GetFunc != nil {
		return m.GetFunc(req)
	}
	return &pb.GetResponse{}, nil
}

func (m *MockGrpcServer) Push(_ context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	if m.PushFunc != nil {
		return m.PushFunc(req)
	}
	return &pb.PushResponse{}, nil
}

func TestGateway_AllBranches(t *testing.T) {
	// Start a local gRPC server for testing remote proxying
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = lis.Close() }()

	grpcServer := grpc.NewServer()
	mockSrv := &MockGrpcServer{}
	pb.RegisterOryxServiceServer(grpcServer, mockSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	addr := lis.Addr().String()

	// 1. Test Gateway getReplicationFactor edge cases
	mcZero := &mesh.Config{NodeID: "local-node", ReplicationFactor: 0, ReplicationFailureMode: mesh.Lenient}
	gwZero := NewGateway(&mesh.NopMesh{}, mcZero, insecure.NewCredentials())
	assert.Equal(t, 1, gwZero.getReplicationFactor())

	mcNeg := &mesh.Config{NodeID: "local-node", ReplicationFactor: -5, ReplicationFailureMode: mesh.Lenient}
	gwNeg := NewGateway(&mesh.NopMesh{}, mcNeg, insecure.NewCredentials())
	assert.Equal(t, 1, gwNeg.getReplicationFactor())

	// 2. Gateway setup with custom mocks
	mc := &mesh.Config{
		NodeID:                 "local-node",
		ReplicationFactor:      3,
		ReplicationFailureMode: mesh.Lenient,
	}
	meshObj := &MockMesher{
		Owners: []kv.NodeID{"local-node", "remote-node"},
		AddrMap: map[kv.NodeID]mesh.PeerAddress{
			"remote-node": mesh.PeerAddress(addr),
		},
	}
	sw := &MockStateWriter{}

	gw := NewGateway(meshObj, mc, insecure.NewCredentials())
	gw.SetStateWriter(sw)
	defer gw.Close()

	// 3. Test Set success (local and remote)
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return &pb.PushResponse{}, nil
	}
	err = gw.Set("test-key", []byte("val"), time.Now().UnixNano())
	assert.NoError(t, err)

	// 4. Test Set error local
	sw.SetErr = errors.New("local set error")
	err = gw.Set("test-key", []byte("val"), time.Now().UnixNano())
	// Should fail because local set fails, but remote might succeed (or not)
	// Actually, both are called in parallel, if all/any fail depending on complete failure logic:
	// "if len(errChan) == len(owners)" means all replicas must fail for Set to return an error.
	// Since we have 2 owners (local and remote), and only local fails, it shouldn't return error.
	assert.NoError(t, err)

	// 5. Test Set all replicas fail
	sw.SetErr = errors.New("local set error")
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return nil, errors.New("remote set error")
	}
	err = gw.Set("test-key", []byte("val"), time.Now().UnixNano())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "direct write replication failed on all replicas")

	// 6. Test Set with empty owners
	meshObj.Owners = nil
	err = gw.Set("test-key", []byte("val"), time.Now().UnixNano())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no replica owners found")
	meshObj.Owners = []kv.NodeID{"local-node", "remote-node"} // restore

	// 7. Test Delete success
	sw.SetErr = nil
	sw.DeleteErr = nil
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return &pb.PushResponse{}, nil
	}
	_, err = gw.Delete("test-key", time.Now().UnixNano())
	assert.NoError(t, err)

	// 8. Test Delete all replicas fail
	sw.DeleteErr = errors.New("local delete error")
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return nil, errors.New("remote delete error")
	}
	_, err = gw.Delete("test-key", time.Now().UnixNano())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "direct delete replication failed on all replicas")

	// 9. Test Delete empty owners
	meshObj.Owners = nil
	_, err = gw.Delete("test-key", time.Now().UnixNano())
	assert.Error(t, err)
	meshObj.Owners = []kv.NodeID{"local-node", "remote-node"} // restore

	// 10. Test Get (from remote peer)
	mockSrv.GetFunc = func(_ *pb.GetRequest) (*pb.GetResponse, error) {
		return &pb.GetResponse{Value: []byte("remote-val"), Exists: true}, nil
	}
	val, ok := gw.Get("test-key")
	assert.True(t, ok)
	assert.Equal(t, []byte("remote-val"), val)

	// 11. Test Get from remote returns not found
	mockSrv.GetFunc = func(_ *pb.GetRequest) (*pb.GetResponse, error) {
		return &pb.GetResponse{Exists: false}, nil
	}
	val, ok = gw.Get("test-key")
	assert.False(t, ok)
	assert.Nil(t, val)

	// 12. Test Gateway proxy methods with address not found
	meshObj.AddrMap = nil // clear address mapping to trigger address not found error
	_, _, err = gw.proxyGetRemote("remote-node", "test-key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "address not found for node")

	err = gw.applySetRemote("remote-node", "test-key", []byte("val"), 0)
	assert.Error(t, err)

	err = gw.applyDeleteRemote("remote-node", "test-key", 0)
	assert.Error(t, err)

	// 13. Test Gateway proxy methods with closed client cache
	meshObj.AddrMap = map[kv.NodeID]mesh.PeerAddress{
		"remote-node": mesh.PeerAddress(addr),
	}
	gw.Close() // close client cache!

	_, _, err = gw.proxyGetRemote("remote-node", "test-key")
	assert.Error(t, err)

	err = gw.applySetRemote("remote-node", "test-key", []byte("val"), 0)
	assert.Error(t, err)

	err = gw.applyDeleteRemote("remote-node", "test-key", 0)
	assert.Error(t, err)
}

// ExistenceStateWriter implements StateWriter plus the optional existenceChecker
// interface the gateway uses to answer DEL accurately.
type ExistenceStateWriter struct {
	MockStateWriter
	Live map[kv.Key]bool
}

func (m *ExistenceStateWriter) Exists(key kv.Key) bool {
	return m.Live[key]
}

func TestGateway_DeleteReportsExistence(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = lis.Close() }()

	grpcServer := grpc.NewServer()
	mockSrv := &MockGrpcServer{
		GetFunc: func(_ *pb.GetRequest) (*pb.GetResponse, error) {
			return &pb.GetResponse{Exists: false}, nil
		},
		PushFunc: func(_ *pb.PushRequest) (*pb.PushResponse, error) {
			return &pb.PushResponse{}, nil
		},
	}
	pb.RegisterOryxServiceServer(grpcServer, mockSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	mc := &mesh.Config{
		NodeID:                 "local-node",
		ReplicationFactor:      2,
		ReplicationFailureMode: mesh.Lenient,
	}
	meshObj := &MockMesher{
		Owners: []kv.NodeID{"local-node", "remote-node"},
		AddrMap: map[kv.NodeID]mesh.PeerAddress{
			"remote-node": mesh.PeerAddress(lis.Addr().String()),
		},
	}

	gw := NewGateway(meshObj, mc, insecure.NewCredentials())
	defer gw.Close()
	gw.SetStateWriter(&ExistenceStateWriter{Live: map[kv.Key]bool{"present": true}})

	// A key held by the local replica reports 1, matching Redis DEL.
	existed, err := gw.Delete("present", time.Now().UnixNano())
	assert.NoError(t, err)
	assert.True(t, existed)

	// A key absent from every replica reports 0 rather than an unconditional 1.
	existed, err = gw.Delete("missing", time.Now().UnixNano())
	assert.NoError(t, err)
	assert.False(t, existed)

	// A key held only by a remote replica is still reported as existing.
	mockSrv.GetFunc = func(_ *pb.GetRequest) (*pb.GetResponse, error) {
		return &pb.GetResponse{Value: []byte("v"), Exists: true}, nil
	}
	existed, err = gw.Delete("remote-only", time.Now().UnixNano())
	assert.NoError(t, err)
	assert.True(t, existed)
}

func TestGateway_UnregisteredStateWriter(t *testing.T) {
	mc := &mesh.Config{
		NodeID:                 "local-node",
		ReplicationFactor:      1,
		ReplicationFailureMode: mesh.Lenient,
	}
	meshObj := &MockMesher{Owners: []kv.NodeID{"local-node"}}

	// No SetStateWriter call: requests arriving before the engine finishes
	// bootstrapping must surface an error instead of a nil dereference panic.
	gw := NewGateway(meshObj, mc, insecure.NewCredentials())
	defer gw.Close()

	assert.NotPanics(t, func() {
		err := gw.Set("key", []byte("val"), time.Now().UnixNano())
		assert.Error(t, err)

		existed, err := gw.Delete("key", time.Now().UnixNano())
		assert.Error(t, err)
		assert.False(t, existed)
	})
}

func TestGateway_StrictReplication(t *testing.T) {
	// Start a local gRPC server for testing remote proxying
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = lis.Close() }()

	grpcServer := grpc.NewServer()
	mockSrv := &MockGrpcServer{}
	pb.RegisterOryxServiceServer(grpcServer, mockSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	addr := lis.Addr().String()

	mc := &mesh.Config{
		NodeID:                 "local-node",
		ReplicationFactor:      2,
		ReplicationFailureMode: mesh.Strict,
	}
	meshObj := &MockMesher{
		Owners: []kv.NodeID{"local-node", "remote-node"},
		AddrMap: map[kv.NodeID]mesh.PeerAddress{
			"remote-node": mesh.PeerAddress(addr),
		},
	}
	sw := &MockStateWriter{}

	gw := NewGateway(meshObj, mc, insecure.NewCredentials())
	gw.SetStateWriter(sw)
	defer gw.Close()

	// 1. Set succeeds when all replicas succeed
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return &pb.PushResponse{}, nil
	}
	err = gw.Set("strict-key", []byte("val"), time.Now().UnixNano())
	assert.NoError(t, err)

	// 2. Set fails if one replica fails (e.g., local succeeds, remote fails)
	sw.SetErr = nil
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return nil, errors.New("remote set error")
	}
	err = gw.Set("strict-key", []byte("val"), time.Now().UnixNano())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "direct write replication failed on replicas")

	// 3. Delete succeeds when all replicas succeed
	sw.DeleteErr = nil
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return &pb.PushResponse{}, nil
	}
	_, err = gw.Delete("strict-key", time.Now().UnixNano())
	assert.NoError(t, err)

	// 4. Delete fails if one replica fails (e.g., local succeeds, remote fails)
	sw.DeleteErr = nil
	mockSrv.PushFunc = func(_ *pb.PushRequest) (*pb.PushResponse, error) {
		return nil, errors.New("remote delete error")
	}
	_, err = gw.Delete("strict-key", time.Now().UnixNano())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "direct delete replication failed on replicas")
}
