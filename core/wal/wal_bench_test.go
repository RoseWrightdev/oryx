package wal

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/security"
)

func BenchmarkWAL_Publish(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "oryx-bench-wal-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	wal, err := NewWal(tmpDir, time.Hour, 1024*1024, 4)
	if err != nil {
		b.Fatal(err)
	}
	wal.Start()
	defer wal.Stop()

	val := kv.Value{Data: []byte("val"), Timestamp: 100, NodeID: "n1"}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		key := fmt.Sprintf("k-%d", i)
		_ = wal.Publish(key, security.HashFunc(key), val)
	}
}

func BenchmarkWAL_Replay(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "oryx-bench-wal-replay-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	wal, err := NewWal(tmpDir, time.Hour, 1024*1024, 4)
	if err != nil {
		b.Fatal(err)
	}
	wal.Start()

	val := kv.Value{Data: []byte("val"), Timestamp: 100, NodeID: "n1"}
	for i := range 10000 {
		key := fmt.Sprintf("k-%d", i)
		_ = wal.Publish(key, security.HashFunc(key), val)
	}
	wal.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		w, _ := NewWal(tmpDir, time.Hour, 1024*1024, 4)
		_, _ = w.Replay()
		w.Stop()
	}
}

func BenchmarkWAL_Clear(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "oryx-bench-wal-clear-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	wal, err := NewWal(tmpDir, time.Hour, 1024*1024, 4)
	if err != nil {
		b.Fatal(err)
	}
	wal.Start()
	defer wal.Stop()

	val := kv.Value{Data: []byte("val"), Timestamp: 100, NodeID: "n1"}
	for i := range 1000 {
		key := fmt.Sprintf("k-%d", i)
		_ = wal.Publish(key, security.HashFunc(key), val)
	}
	offsets, _ := wal.PrepareSnapshot()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = wal.Clear(offsets)
	}
}
