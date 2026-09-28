// Package oryx provides a highly-concurrent, partitioned key-value database engine.
package oryx

import (
	"time"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/cluster"
	"github.com/rosewrightdev/oryx/cluster/entropy"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/core"
	"github.com/rosewrightdev/oryx/core/clock"
	"github.com/rosewrightdev/oryx/core/evict"
	"github.com/rosewrightdev/oryx/kv"
	"google.golang.org/grpc/credentials"
)

// Database defines the core storage and replication database interface of the oryx node.
type Database interface {
	Get(key kv.Key) ([]byte, bool)
	Set(key kv.Key, value []byte) error
	Delete(key kv.Key) (bool, error)
	Owner(key kv.Key) kv.NodeID
	NodeID() kv.NodeID
	Start()
	Stop()
	SyncPull(pullConfig *entropy.PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error)
	SyncPush(sets []*pb.SetRequest, deletes []*pb.DeleteRequest) error
	Addr() string
	GossipAddr() string
	Mesh() mesh.Mesher
	Creds() credentials.TransportCredentials
}

// DatabaseConfig specifies the parameters required to initialize and run a oryx Database.
type DatabaseConfig struct {
	evt             evict.Evictor
	clock           clock.Clocker
	creds           credentials.TransportCredentials
	walPath         string
	snpPath         string
	meshConfig      mesh.Config
	walInterval     time.Duration
	snpInterval     time.Duration
	walSegments     int
	gossipInterval  time.Duration
	walBufferSize   uint32
	disableWal      bool
	disableSnapshot bool
}

func newDatabase(config DatabaseConfig) (Database, error) {
	coreConfig := core.Config{
		Evt:             config.evt,
		Clock:           config.clock,
		WalPath:         config.walPath,
		SnpPath:         config.snpPath,
		WalInterval:     config.walInterval,
		SnpInterval:     config.snpInterval,
		WalSegments:     config.walSegments,
		WalBufferSize:   config.walBufferSize,
		NodeID:          config.meshConfig.NodeID,
		DisableWal:      config.disableWal,
		DisableSnapshot: config.disableSnapshot,
	}

	coreEng, err := core.NewEngine(coreConfig)
	if err != nil {
		return nil, err
	}

	if config.meshConfig.SingleNode {
		return &singleNodeAdapter{
			Engine: coreEng,
			config: config.meshConfig,
			creds:  config.creds,
		}, nil
	}

	clusterConfig := cluster.Config{
		MeshConfig:     config.meshConfig,
		Creds:          config.creds,
		GossipInterval: config.gossipInterval,
	}

	node, err := cluster.NewNode(coreEng, clusterConfig)
	if err != nil {
		return nil, err
	}

	return node, nil
}
