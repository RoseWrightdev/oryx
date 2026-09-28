package wal

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx/kv"
)

func FuzzWalReplay(f *testing.F) {
	// Build seed corpus with valid WAL segment data
	var buf bytes.Buffer

	// Seed 1: Set
	key1 := []byte("user:1")
	node1 := []byte("node-1")
	val1 := []byte("value1")
	payloadLen1 := 1 + 8 + 2 + len(key1) + 2 + len(node1) + 4 + len(val1)
	hdr1 := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr1, uint32(payloadLen1))
	buf.Write(hdr1)
	buf.WriteByte(opSet)
	var ts1 [8]byte
	binary.BigEndian.PutUint64(ts1[:], 100)
	buf.Write(ts1[:])
	var klen1 [2]byte
	binary.BigEndian.PutUint16(klen1[:], uint16(len(key1)))
	buf.Write(klen1[:])
	buf.Write(key1)
	var nlen1 [2]byte
	binary.BigEndian.PutUint16(nlen1[:], uint16(len(node1)))
	buf.Write(nlen1[:])
	buf.Write(node1)
	var dlen1 [4]byte
	binary.BigEndian.PutUint32(dlen1[:], uint32(len(val1)))
	buf.Write(dlen1[:])
	buf.Write(val1)

	// Seed 2: Delete
	key2 := []byte("user:2")
	node2 := []byte("node-1")
	payloadLen2 := 1 + 8 + 2 + len(key2) + 2 + len(node2)
	hdr2 := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr2, uint32(payloadLen2))
	buf.Write(hdr2)
	buf.WriteByte(opDelete)
	var ts2 [8]byte
	binary.BigEndian.PutUint64(ts2[:], 101)
	buf.Write(ts2[:])
	var klen2 [2]byte
	binary.BigEndian.PutUint16(klen2[:], uint16(len(key2)))
	buf.Write(klen2[:])
	buf.Write(key2)
	var nlen2 [2]byte
	binary.BigEndian.PutUint16(nlen2[:], uint16(len(node2)))
	buf.Write(nlen2[:])
	buf.Write(node2)

	f.Add([]byte{})
	f.Add(buf.Bytes())
	f.Add([]byte{0x00, 0x00, 0x00, 0x10, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		tempDir := t.TempDir()
		segPath := filepath.Join(tempDir, "seg_00.log")

		if err := os.WriteFile(segPath, data, 0600); err != nil {
			return
		}

		wal, err := NewWal(tempDir, 100*time.Millisecond, 4096, 1)
		if err != nil {
			return
		}
		defer wal.Stop()

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("replaySegment panicked on input %x: %v", data, r)
			}
		}()

		results := make(map[kv.Key]kv.Value)
		var mu sync.Mutex
		_ = wal.replaySegment(wal.segments[0], results, &mu)
	})
}
