package evict

import (
	"fmt"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx/kv"
)

func BenchmarkEviction_Publish(b *testing.B) {
	evt := NewLRU(LRUConfig{
		Capacity:   1000,
		TTL:        time.Hour,
		ShardCount: 16,
	})
	evt.Start()
	defer evt.Stop()
	evt.SetEvictCallback(func(_ kv.Key, _ Reason) error {
		return nil
	})

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		evt.Publish(fmt.Sprintf("key-%d", i), uint64(i))
		i++
	}
}

func BenchmarkEviction_PublishDelete(b *testing.B) {
	evt := NewLRU(LRUConfig{
		Capacity:   1000,
		TTL:        time.Hour,
		ShardCount: 16,
	})
	evt.Start()
	defer evt.Stop()

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		evt.PublishDelete(fmt.Sprintf("key-%d", i), uint64(i))
		i++
	}
}
