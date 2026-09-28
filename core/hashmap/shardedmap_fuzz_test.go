package hashmap

import (
	"fmt"
	"testing"

	"github.com/rosewrightdev/oryx/kv"
)

func FuzzStoreLWWConvergence(f *testing.F) {
	// Seed corpus with operation sequences
	f.Add("k1", []byte("v1"), int64(100), "node-1", false)
	f.Add("k1", []byte("v2"), int64(101), "node-2", false)
	f.Add("k1", []byte("v3"), int64(100), "node-2", true)
	f.Add("k2", []byte("v1"), int64(200), "node-1", false)

	f.Fuzz(func(t *testing.T, keyStr string, valBytes []byte, ts int64, nodeIDStr string, tombstone bool) {
		if len(keyStr) == 0 {
			return
		}

		key := kv.Key(keyStr)
		val := kv.Value{
			Data:      valBytes,
			Timestamp: ts,
			NodeID:    nodeIDStr,
			Tombstone: tombstone,
		}

		sm1 := NewShardedMap()
		sm2 := NewShardedMap()

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("StoreLWW panicked on input key=%s ts=%d: %v", keyStr, ts, r)
			}
		}()

		// Apply to sm1
		sm1.PutLWW(key, val)
		// Apply same mutation to sm2
		sm2.PutLWW(key, val)

		// Assert RootDigests are equal after identical state updates
		if sm1.RootDigest() != sm2.RootDigest() {
			t.Fatalf("RootDigest divergence after identical update: sm1=%d, sm2=%d", sm1.RootDigest(), sm2.RootDigest())
		}
	})
}

func FuzzShardedMapRandomOps(f *testing.F) {
	f.Add([]byte("k1:v1:100:node1:0;k2:v2:200:node2:0;k1::201:node1:1;"))

	f.Fuzz(func(t *testing.T, opsScript []byte) {
		sm := NewShardedMap()

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("ShardedMap panicked on ops script %s: %v", string(opsScript), r)
			}
		}()

		// Execute synthetic operation stream
		for i := 0; i < len(opsScript); i += 10 {
			key := kv.Key(fmt.Sprintf("k%d", i%16))
			val := kv.Value{
				Data:      opsScript[i:],
				Timestamp: int64(i * 100),
				NodeID:    fmt.Sprintf("node-%d", i%4),
				Tombstone: i%3 == 0,
			}
			_ = sm.PutLWW(key, val)
			_, _ = sm.Get(key)
		}
	})
}
