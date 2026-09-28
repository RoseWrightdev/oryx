package oryx

import (
	"fmt"

	pb "github.com/rosewrightdev/oryx/api"
	"github.com/rosewrightdev/oryx/cluster/entropy"
	"github.com/rosewrightdev/oryx/cluster/mesh"
	"github.com/rosewrightdev/oryx/core"
	"github.com/rosewrightdev/oryx/kv"
	"google.golang.org/grpc/credentials"
)

type singleNodeAdapter struct {
	core.Engine
	config mesh.Config
	creds  credentials.TransportCredentials
}

func (s *singleNodeAdapter) Core() core.Engine {
	return s.Engine
}

func (s *singleNodeAdapter) Owner(_ kv.Key) kv.NodeID {
	return s.config.NodeID
}

func (s *singleNodeAdapter) NodeID() kv.NodeID {
	return s.config.NodeID
}

func (s *singleNodeAdapter) SyncPull(_ *entropy.PullConfig) ([]*pb.SetRequest, []*pb.DeleteRequest, error) {
	return nil, nil, nil
}

func (s *singleNodeAdapter) SyncPush(_ []*pb.SetRequest, _ []*pb.DeleteRequest) error {
	return nil
}

func (s *singleNodeAdapter) Addr() string {
	if s.config.BindAddr == "" {
		panic("oryx: bind address not configured")
	}
	return fmt.Sprintf("%s:%d", s.config.BindAddr, s.config.GrpcPort)
}

func (s *singleNodeAdapter) GossipAddr() string {
	if s.config.BindAddr == "" {
		panic("oryx: bind address not configured")
	}
	return fmt.Sprintf("%s:%d", s.config.BindAddr, s.config.BindPort)
}

func (s *singleNodeAdapter) Mesh() mesh.Mesher {
	return &mesh.NopMesh{}
}

func (s *singleNodeAdapter) Creds() credentials.TransportCredentials {
	return s.creds
}
