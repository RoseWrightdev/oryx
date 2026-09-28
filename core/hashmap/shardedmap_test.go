package hashmap

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx/kv"
	"github.com/stretchr/testify/assert"
)

func TestShardedMap_Basic(t *testing.T) {
	sm := NewShardedMap()

	key := "test"
	val := kv.Value{Data: []byte("val"), Timestamp: 123}

	sm.Put(key, val)

	got, ok := sm.GetRecord(key)
	assert.True(t, ok)
	assert.Equal(t, val, got)

	sm.Delete(key)
	val, ok = sm.GetRecord(key)
	assert.Nil(t, val.Data)
	assert.False(t, ok)
}

func TestShardedMap_Digests(t *testing.T) {
	sm := NewShardedMap()

	// Ensure we put things in different shards by manually picking hashes
	sm.store("a", 0, kv.Value{Timestamp: 1})
	sm.store("b", 1, kv.Value{Timestamp: 1})

	digests := make(map[ShardID]ShardDigest)
	for i := range ShardCount {
		digests[ShardID(i)] = make([]Digest, SubBucketCount)
	}
	sm.FillDigests(digests)
	assert.Len(t, digests, int(ShardCount))
	assert.NotEqual(t, digests[0], digests[1])

	// An empty shard's sub-hashes are a fixed non-zero fingerprint
	// (mixCount(0) in every slot), not literally zero: see #85.
	emptyDigest := make([]Digest, SubBucketCount)
	for i := range emptyDigest {
		emptyDigest[i] = mixCount(0)
	}
	assert.Equal(t, emptyDigest, digests[2], "empty shard's sub-hashes should be the empty-bucket fingerprint")

	// Verify sub-bucket indexing for shard 0
	// hash 0 maps to shard 0, subIndex 0
	assert.NotEqual(t, uint64(0), digests[0][0])
}

// TestShardedMap_EmptyBucketDistinctFromCancelledNonEmpty pins #85: a
// cancelled-to-zero bucket must not read as identical to a genuinely empty one.
func TestShardedMap_EmptyBucketDistinctFromCancelledNonEmpty(t *testing.T) {
	sm := NewShardedMap()

	emptyDigests := make(map[ShardID]ShardDigest)
	for i := range ShardCount {
		emptyDigests[ShardID(i)] = make([]Digest, SubBucketCount)
	}
	sm.FillDigests(emptyDigests)
	emptyFingerprint := emptyDigests[0][0]

	// Simulate 3 items whose hashes happen to XOR to zero, same as an empty bucket.
	sm[0].mu.Lock()
	sm[0].subDigests[0] = 0
	sm[0].subCounts[0] = 3
	sm[0].mu.Unlock()

	cancelled := make(map[ShardID]ShardDigest)
	for i := range ShardCount {
		cancelled[ShardID(i)] = make([]Digest, SubBucketCount)
	}
	sm.FillDigests(cancelled)

	assert.NotEqual(t, emptyFingerprint, cancelled[0][0],
		"a non-empty bucket whose hashes cancel to zero must not read as empty")
}

// TestShardedMap_CountTrackingReturnsToEmpty checks count bookkeeping across
// insert, in-place update, and delete converges back to the empty fingerprint.
func TestShardedMap_CountTrackingReturnsToEmpty(t *testing.T) {
	sm := NewShardedMap()
	key, hash := kv.Key("count-key"), kv.HashKey(0)

	digests := make(map[ShardID]ShardDigest)
	for i := range ShardCount {
		digests[ShardID(i)] = make([]Digest, SubBucketCount)
	}
	sm.FillDigests(digests)
	emptyFingerprint := digests[0][0]

	sm.store(key, hash, kv.Value{Data: []byte("v1"), Timestamp: 1})
	sm.FillDigests(digests)
	afterInsert := digests[0][0]
	assert.NotEqual(t, emptyFingerprint, afterInsert)

	// An in-place update (same key, still present) must not change the count.
	sm.store(key, hash, kv.Value{Data: []byte("v2"), Timestamp: 2})
	sm.FillDigests(digests)
	assert.NotEqual(t, afterInsert, digests[0][0], "digest should change with the value")
	assert.NotEqual(t, emptyFingerprint, digests[0][0], "count-derived component must be unchanged by an update")

	sm.deleteKey(key, hash)
	sm.FillDigests(digests)
	assert.Equal(t, emptyFingerprint, digests[0][0], "deleting the only key must return to the empty fingerprint")
}

func TestShardedMap_Concurrency(t *testing.T) {
	sm := NewShardedMap()
	const (
		keys       = 100
		goroutines = 10
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			for k := range keys {
				key := fmt.Sprintf("k-%d", k)
				sm.Put(key, kv.Value{Timestamp: int64(id)})
			}
		}(i)
	}

	wg.Wait()

	// Check random key
	key := "k-50"
	v, ok := sm.GetRecord(key)
	assert.True(t, ok)
	assert.GreaterOrEqual(t, v.Timestamp, int64(0))
}

// TestShardedMap_StoreLWWSameNodeSameTimestamp covers the LWW tie that arises
// when two writes carry both the same timestamp and the same NodeID — rapid
// consecutive writes from one node, or a client that supplies its own
// timestamp.
func TestShardedMap_StoreLWWSameNodeSameTimestamp(t *testing.T) {
	key := "same-node"
	first := kv.Value{Data: []byte("first"), Timestamp: 100, NodeID: "node-1"}
	second := kv.Value{Data: []byte("second"), Timestamp: 100, NodeID: "node-1"}

	apply := func(a, b kv.Value) (accepted bool, final kv.Value) {
		sm := NewShardedMap()
		sm.PutLWW(key, a)
		accepted = sm.PutLWW(key, b)
		final, _ = sm.GetRecord(key)
		return accepted, final
	}

	forwardAccepted, forwardFinal := apply(first, second)
	reverseAccepted, reverseFinal := apply(second, first)

	// Exactly one ordering accepts its second write; previously neither did.
	assert.NotEqual(t, forwardAccepted, reverseAccepted,
		"one of the two orderings must accept its second write")

	// Both orderings settle on the same value, so replicas that observed the
	// pair in different orders still converge instead of flip-flopping.
	assert.Equal(t, forwardFinal, reverseFinal)

	// Re-applying a byte-identical write is a genuine no-op.
	sm := NewShardedMap()
	assert.True(t, sm.PutLWW(key, first))
	before := sm.RootDigest()
	assert.False(t, sm.PutLWW(key, first))
	assert.Equal(t, before, sm.RootDigest())

	// Ordinary LWW ordering is untouched.
	assert.False(t, sm.PutLWW(key, kv.Value{Data: []byte("older"), Timestamp: 99, NodeID: "node-1"}))
	assert.True(t, sm.PutLWW(key, kv.Value{Data: []byte("newer"), Timestamp: 101, NodeID: "node-1"}))
	assert.False(t, sm.PutLWW(key, kv.Value{Data: []byte("lower-node"), Timestamp: 101, NodeID: "node-0"}))
	assert.True(t, sm.PutLWW(key, kv.Value{Data: []byte("higher-node"), Timestamp: 101, NodeID: "node-2"}))
}

// TestShardedMap_FillDigestsAllocatesDestination pins that a caller handing in
// a plain map gets it populated. copy() into a nil slice moves zero elements,
// so the previous implementation returned an empty map without any error.
func TestShardedMap_FillDigestsAllocatesDestination(t *testing.T) {
	sm := NewShardedMap()
	sm.store("a", 0, kv.Value{Timestamp: 1})

	dst := make(map[ShardID]ShardDigest)
	sm.FillDigests(dst)

	assert.Len(t, dst, int(ShardCount))
	for id, buckets := range dst {
		assert.Len(t, buckets, int(SubBucketCount), "shard %d", id)
	}
	assert.NotEqual(t, uint64(0), dst[0][0], "digest for the populated bucket must be copied through")

	// A pre-sized destination is reused in place rather than reallocated.
	preSized := make(map[ShardID]ShardDigest, ShardCount)
	for i := range ShardCount {
		preSized[ShardID(i)] = make(ShardDigest, SubBucketCount)
	}
	original := preSized[0]
	sm.FillDigests(preSized)
	assert.Equal(t, dst, preSized)
	assert.Equal(t, &original[0], &preSized[0][0], "pre-sized buffers must be filled in place")

	// A nil destination is a no-op rather than a panic.
	assert.NotPanics(t, func() { sm.FillDigests(nil) })
}

// TestShardedMap_RangeDoesNotBlockWriters checks that the callback runs without
// the shard lock held. Range's real callers encode to disk or to the network,
// so holding shard.mu across them stalled every writer on that shard. If the
// lock is held, the Store below cannot complete and the test reports it.
func TestShardedMap_RangeDoesNotBlockWriters(t *testing.T) {
	sm := NewShardedMap()

	for i := range 256 {
		key := kv.Key(fmt.Sprintf("range-key-%d", i))
		sm.Put(key, kv.Value{Data: []byte("v"), Timestamp: 1, NodeID: "node-1"})
	}

	var once sync.Once
	blocked := false

	sm.Range(func(key kv.Key, _ kv.Value) bool {
		once.Do(func() {
			// Write into the very shard being iterated, from another goroutine.
			done := make(chan struct{})
			go func() {
				defer close(done)
				sm.Put(key, kv.Value{Data: []byte("written-during-range"), Timestamp: 2, NodeID: "node-1"})
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				blocked = true
			}
		})
		return true
	})

	assert.False(t, blocked, "Range held the shard lock while the callback ran, blocking a concurrent Store")

	// Iteration still visited the whole map.
	seen := 0
	sm.Range(func(_ kv.Key, _ kv.Value) bool {
		seen++
		return true
	})
	assert.Equal(t, 256, seen)

	// Early termination still works.
	seen = 0
	sm.Range(func(_ kv.Key, _ kv.Value) bool {
		seen++
		return false
	})
	assert.Equal(t, 1, seen)
}
