package oryx

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/core/clock"
	"github.com/rosewrightdev/oryx/core/evict"
	"github.com/rosewrightdev/oryx/kv"
)

// DatabaseBuilder provides a fluent API for constructing and configuring a oryx database.
type DatabaseBuilder struct {
	evt             evict.Evictor
	clock           clock.Clocker
	creds           credentials.TransportCredentials
	meshBuilder     *mesh.ConfigBuilder
	walPath         string
	snpPath         string
	walInterval     time.Duration
	snpInterval     time.Duration
	walSegments     int
	gossipInterval  time.Duration
	walBufferSize   uint32
	disableWal      bool
	disableSnapshot bool
}

// NewDatabaseBuilder initializes a new DatabaseBuilder instance with default sub-builders.
func NewDatabaseBuilder() *DatabaseBuilder {
	return &DatabaseBuilder{
		meshBuilder: mesh.NewConfigBuilder(),
	}
}

// NewDefaultDatabase constructs a default oryx database configuration.
func NewDefaultDatabase(walPath, snpPath string) (Database, error) {
	return NewDatabaseBuilder().Default().SetWalPath(walPath).SetSnpPath(snpPath).Build()
}

// Default populates the DatabaseBuilder with sensible default values.
func (eb *DatabaseBuilder) Default() *DatabaseBuilder {
	eb.walInterval = 500 * time.Millisecond
	eb.snpInterval = 5 * time.Minute
	eb.walBufferSize = 64 * 1024
	eb.walSegments = 16
	eb.evt = evict.NewLRU(evict.LRUConfig{Capacity: 10000, TTL: 24 * time.Hour, ShardCount: 16})
	eb.clock = clock.NewClock()
	eb.gossipInterval = 10 * time.Second
	eb.meshBuilder = mesh.NewConfigBuilder()

	// Autoselect NodeID to a SHA-256 hash
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	hash := sha256.Sum256(b)
	nodeID := hex.EncodeToString(hash[:])
	eb.meshBuilder.SetNodeID(kv.NodeID(nodeID))

	// Find dynamic next available ports
	bindAddr := "127.0.0.1"
	gossipPort := nextAvailablePort(bindAddr, 7946)
	grpcPort := nextAvailablePort(bindAddr, 50051)

	eb.meshBuilder.SetBindAddr(bindAddr)
	eb.meshBuilder.SetBindPort(gossipPort)
	eb.meshBuilder.SetGrpcPort(grpcPort)
	eb.meshBuilder.SetReplicationFactor(3)
	eb.meshBuilder.SetReplicationFailureMode(mesh.Strict)

	eb.walPath = "data/wal"
	eb.snpPath = "data/snapshot.bin"
	return eb
}

// DisableWal disables the Write-Ahead Log for volatile in-memory operation.
func (eb *DatabaseBuilder) DisableWal(disabled bool) *DatabaseBuilder {
	eb.disableWal = disabled
	return eb
}

// DisableSnapshot disables background binary snapshotting.
func (eb *DatabaseBuilder) DisableSnapshot(disabled bool) *DatabaseBuilder {
	eb.disableSnapshot = disabled
	return eb
}

// VolatileMode enables 100% volatile in-memory operation with 0 disk I/O.
func (eb *DatabaseBuilder) VolatileMode() *DatabaseBuilder {
	eb.disableWal = true
	eb.disableSnapshot = true
	eb.walPath = "nop"
	eb.snpPath = "nop"
	return eb
}

// SetWalPath sets the path to the write-ahead log directory.
func (eb *DatabaseBuilder) SetWalPath(path string) *DatabaseBuilder {
	eb.walPath = path
	return eb
}

// SetSnpPath sets the path to the snapshot file.
func (eb *DatabaseBuilder) SetSnpPath(path string) *DatabaseBuilder {
	eb.snpPath = path
	return eb
}

// SetSnpInterval sets the snapshot interval.
func (eb *DatabaseBuilder) SetSnpInterval(interval time.Duration) *DatabaseBuilder {
	eb.snpInterval = interval
	return eb
}

// SetWalInterval sets the sync interval for the write-ahead log.
func (eb *DatabaseBuilder) SetWalInterval(interval time.Duration) *DatabaseBuilder {
	eb.walInterval = interval
	return eb
}

// SetWalBufferSize sets the buffer size for the write-ahead log.
func (eb *DatabaseBuilder) SetWalBufferSize(size uint32) *DatabaseBuilder {
	eb.walBufferSize = size
	return eb
}

// SetWalSegments sets the maximum number of log segments.
func (eb *DatabaseBuilder) SetWalSegments(count int) *DatabaseBuilder {
	eb.walSegments = count
	return eb
}

// SetEvictor sets the eviction service instance.
func (eb *DatabaseBuilder) SetEvictor(evt evict.Evictor) *DatabaseBuilder {
	eb.evt = evt
	return eb
}

// SetClock sets the clock implementation for generating timestamps.
func (eb *DatabaseBuilder) SetClock(clock clock.Clocker) *DatabaseBuilder {
	eb.clock = clock
	return eb
}

// SetCluster sets the cluster configuration builder.
func (eb *DatabaseBuilder) SetCluster(cb *mesh.ConfigBuilder) *DatabaseBuilder {
	eb.meshBuilder = cb
	return eb
}

// Proxy methods for MeshConfigBuilder
// These allow for a flatter API while maintaining modularity under the hood.

// SetNodeID sets the unique node ID for cluster identity.
func (eb *DatabaseBuilder) SetNodeID(id kv.NodeID) *DatabaseBuilder {
	eb.meshBuilder.SetNodeID(id)
	return eb
}

// SetReplicationFactor sets the replication factor for the cluster.
func (eb *DatabaseBuilder) SetReplicationFactor(n int) *DatabaseBuilder {
	eb.meshBuilder.SetReplicationFactor(n)
	return eb
}

func (eb *DatabaseBuilder) SetReplicationFailureMode(m mesh.ReplicationFailureMode) *DatabaseBuilder {
	eb.meshBuilder.SetReplicationFailureMode(m)
	return eb
}

// SetBindAddr sets the bind address for gossip membership.
func (eb *DatabaseBuilder) SetBindAddr(addr string) *DatabaseBuilder {
	eb.meshBuilder.SetBindAddr(addr)
	return eb
}

// SetBindPort sets the bind port for gossip membership.
func (eb *DatabaseBuilder) SetBindPort(port int) *DatabaseBuilder {
	eb.meshBuilder.SetBindPort(port)
	return eb
}

// SetAdvertiseAddr sets the address advertised to other cluster nodes.
func (eb *DatabaseBuilder) SetAdvertiseAddr(addr string) *DatabaseBuilder {
	eb.meshBuilder.SetAdvertiseAddr(addr)
	return eb
}

// SetSeedNodes sets the seed nodes to join upon startup.
func (eb *DatabaseBuilder) SetSeedNodes(seeds []string) *DatabaseBuilder {
	eb.meshBuilder.SetSeedNodes(seeds)
	return eb
}

// SetGrpcPort sets the gRPC API port.
func (eb *DatabaseBuilder) SetGrpcPort(port int) *DatabaseBuilder {
	eb.meshBuilder.SetGrpcPort(port)
	return eb
}

// SingleNode configures the database to run in single-node mode.
func (eb *DatabaseBuilder) SingleNode() *DatabaseBuilder {
	eb.meshBuilder.SingleNode()
	return eb
}

// SetGossipInterval sets the gossip communication interval.
func (eb *DatabaseBuilder) SetGossipInterval(interval time.Duration) *DatabaseBuilder {
	eb.gossipInterval = interval
	return eb
}

// SetCreds sets the transport credentials for secure node-to-node connections.
func (eb *DatabaseBuilder) SetCreds(creds credentials.TransportCredentials) *DatabaseBuilder {
	eb.creds = creds
	return eb
}

// SetInsecure configures insecure gRPC connections for development.
func (eb *DatabaseBuilder) SetInsecure() *DatabaseBuilder {
	eb.creds = insecure.NewCredentials()
	return eb
}

// FastTest optimizes cluster parameters for quick unit/integration testing.
func (eb *DatabaseBuilder) FastTest() *DatabaseBuilder {
	eb.meshBuilder.EnableFastTest()
	return eb
}

// Build validates the configuration and returns a new Database instance.
func (eb *DatabaseBuilder) Build() (Database, error) {
	if isZero(eb.walPath) {
		return nil, fmt.Errorf("required eb.walPath is unset; configure eb.walPath with SetWalPath(path string)")
	}

	if isZero(eb.snpPath) {
		return nil, fmt.Errorf("required eb.snpPath is unset; configure eb.snpPath with SetSnpPath(path string)")
	}

	if isZero(eb.walInterval) {
		return nil, fmt.Errorf("required eb.walInterval is unset; configure eb.walInterval with SetWalInterval(interval time.Duration)")
	}

	if isZero(eb.snpInterval) {
		return nil, fmt.Errorf("required eb.snpInterval is unset; configure eb.snpInterval with SetSnpInterval(interval time.Duration)")
	}

	if eb.creds == nil {
		return nil, fmt.Errorf("transport credentials are required; use Setcreds(creds) or SetInsecure() for development")
	}

	if isZero(eb.walBufferSize) {
		return nil, fmt.Errorf("required eb.walBufferSize is unset; configure eb.walBufferSize with SetWalBufferSize(size uint32)")
	}

	if isZero(eb.walSegments) {
		return nil, fmt.Errorf("required eb.walSegments is unset; configure eb.walSegments with SetWalSegments(count int)")
	}

	if eb.clock == nil {
		return nil, fmt.Errorf("required eb.clock is unset; configure eb.clock with SetClock(clock clock.Clocker)")
	}

	meshConfig := eb.meshBuilder.Build()

	// Addr()/GossipAddr() panic on an empty BindAddr regardless of mode, so
	// validate it here rather than at first use (#76).
	if meshConfig.BindAddr == "" {
		return nil, fmt.Errorf("required BindAddr is unset; configure it via SetBindAddr(addr string)")
	}

	if !meshConfig.SingleNode {
		// GrpcPort 0 is allowed for dynamic allocation (e.g., in tests)
		if isZero(eb.gossipInterval) {
			return nil, fmt.Errorf("required eb.gossipInterval is unset for distributed mode; configure it via SetGossipInterval")
		}
		if meshConfig.ReplicationFailureMode == "" {
			return nil, fmt.Errorf("required ReplicationFailureMode is unset for distributed mode; configure it via SetReplicationFailureMode")
		}
	}

	config := DatabaseConfig{
		walPath:         eb.walPath,
		snpPath:         eb.snpPath,
		walInterval:     eb.walInterval,
		snpInterval:     eb.snpInterval,
		walBufferSize:   eb.walBufferSize,
		walSegments:     eb.walSegments,
		evt:             eb.evt,
		clock:           eb.clock,
		meshConfig:      meshConfig,
		gossipInterval:  eb.gossipInterval,
		creds:           eb.creds,
		disableWal:      eb.disableWal,
		disableSnapshot: eb.disableSnapshot,
	}

	return newDatabase(config)
}

func isZero[T comparable](val T) bool {
	var zero T
	return zero == val
}

// nextAvailablePort asks the OS for a free port via :0 instead of probing a
// fixed range, so concurrent callers no longer land on the same port (#75).
func nextAvailablePort(addr string, startPort int) int {
	if lis, err := net.Listen("tcp", fmt.Sprintf("%s:0", addr)); err == nil {
		port := lis.Addr().(*net.TCPAddr).Port
		_ = lis.Close()
		return port
	}
	for port := startPort; port < startPort+1000; port++ {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
		if err == nil {
			_ = lis.Close()
			return port
		}
	}
	return startPort
}
