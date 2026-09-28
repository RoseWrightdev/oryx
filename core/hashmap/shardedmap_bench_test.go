package hashmap

import (
	"fmt"
	"testing"

	"github.com/rosewrightdev/oryx/kv"
)

func BenchmarkShardedMap_RootDigest(b *testing.B) {
	sm := NewShardedMap()
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		sm.Put(key, kv.Value{
			Data:      []byte("value"),
			Timestamp: int64(i),
		})
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = sm.RootDigest()
	}
}

func BenchmarkShardedMap_FillShardDigests(b *testing.B) {
	sm := NewShardedMap()
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		sm.Put(key, kv.Value{
			Data:      []byte("value"),
			Timestamp: int64(i),
		})
	}
	shards := make(map[ShardID]Digest)

	b.ReportAllocs()
	for b.Loop() {
		sm.FillShardDigests(shards)
	}
}

func BenchmarkShardedMap_FillDigests(b *testing.B) {
	sm := NewShardedMap()
	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		sm.Put(key, kv.Value{
			Data:      []byte("value"),
			Timestamp: int64(i),
		})
	}
	buckets := make(map[ShardID]ShardDigest)
	for i := range ShardCount {
		buckets[ShardID(i)] = make([]Digest, SubBucketCount)
	}

	b.ReportAllocs()
	for b.Loop() {
		sm.FillDigests(buckets)
	}
}

func BenchmarkShardedMap_StoreUpdate(b *testing.B) {
	sm := NewShardedMap()
	key := "test-key"

	// Pre-fill the key
	sm.Put(key, kv.Value{
		NodeID:    "node-1",
		Data:      []byte("some-value-payload-of-reasonable-size"),
		Timestamp: 100,
	})

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		sm.Put(key, kv.Value{
			NodeID:    "node-1",
			Data:      []byte("some-value-payload-of-reasonable-size"),
			Timestamp: int64(i + 101),
		})
	}
}

func BenchmarkShardedMap_Delete(b *testing.B) {
	sm := NewShardedMap()
	key := "test-key"

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		sm.Put(key, kv.Value{
			NodeID:    "node-1",
			Data:      []byte("some-value-payload-of-reasonable-size"),
			Timestamp: int64(i),
		})
		sm.Delete(key)
	}
}
