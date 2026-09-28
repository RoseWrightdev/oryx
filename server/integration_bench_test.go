package server

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx"
	"github.com/rosewrightdev/oryx/kv"
)

func BenchmarkClusterIntegration_ReadProxy(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "oryx-bench-int-*")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// Setup Node 1
	n1Dir := filepath.Join(tmpDir, "node1")
	_ = os.MkdirAll(n1Dir, 0750)

	mlLis1, _ := net.Listen("tcp", "127.0.0.1:0")
	mlPort1 := mlLis1.Addr().(*net.TCPAddr).Port
	_ = mlLis1.Close()

	gLis1, _ := net.Listen("tcp", "127.0.0.1:0")
	grpcPort1 := gLis1.Addr().(*net.TCPAddr).Port
	_ = gLis1.Close()

	e1, _ := oryx.NewDatabaseBuilder().
		Default().
		FastTest().
		SetWalPath(filepath.Join(n1Dir, "wal")).
		SetSnpPath(filepath.Join(n1Dir, "snp.gob")).
		SetNodeID(kv.NodeID("node1")).
		SetBindPort(mlPort1).
		SetGrpcPort(grpcPort1).
		SetInsecure().
		SetReplicationFactor(2).
		Build()
	e1.Start()
	defer e1.Stop()

	s1 := NewServer(e1)
	go func() { _ = s1.Run() }()
	defer s1.Stop()

	// Setup Node 2
	n2Dir := filepath.Join(tmpDir, "node2")
	_ = os.MkdirAll(n2Dir, 0750)

	gLis2, _ := net.Listen("tcp", "127.0.0.1:0")
	grpcPort2 := gLis2.Addr().(*net.TCPAddr).Port
	_ = gLis2.Close()

	e2, _ := oryx.NewDatabaseBuilder().
		Default().
		FastTest().
		SetWalPath(filepath.Join(n2Dir, "wal")).
		SetSnpPath(filepath.Join(n2Dir, "snp.gob")).
		SetNodeID(kv.NodeID("node2")).
		SetBindPort(0).
		SetGrpcPort(grpcPort2).
		SetSeedNodes([]string{fmt.Sprintf("127.0.0.1:%d", mlPort1)}).
		SetInsecure().
		SetReplicationFactor(2).
		Build()
	e2.Start()
	defer e2.Stop()

	s2 := NewServer(e2)
	go func() { _ = s2.Run() }()
	defer s2.Stop()

	// Wait for nodes to discover each other
	time.Sleep(500 * time.Millisecond)

	// Write some keys on node1
	for i := range 100 {
		_ = e1.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		// Node 2 proxies the read to Node 1
		_, _ = e2.Get(kv.Key(fmt.Sprintf("key-%d", i%100)))
		i++
	}
}
