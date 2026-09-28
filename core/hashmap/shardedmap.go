// Package hashmap provides a concurrent-safe, high-performance sharded map implementation.
package hashmap

import (
	"math/bits"
	"sync"

	"github.com/rosewrightdev/oryx/kv"
	"github.com/rosewrightdev/oryx/security"
)

// ShardID represents the identifier of a shard within the ShardedMap.
type ShardID = int32

// Digest represents a hash value used for detecting data divergence.
type Digest = uint64

// RootDigest represents the combined hash of the entire database state.
type RootDigest = uint64

// ShardDigest represents the collection of sub-bucket digests for a specific shard.
type ShardDigest = []Digest

// SubBucketCount defines the number of sub-buckets in each shard.
const SubBucketCount = 64

// ShardCount defines the total number of shards in the map.
const ShardCount = 128

// shard is a single thread-safe bucket within the ShardedMap.
type shard struct {
	buckets     [SubBucketCount]map[kv.Key]kv.Value
	subDigests  [SubBucketCount]Digest
	subCounts   [SubBucketCount]uint32
	shardDigest Digest
	shardCount  uint32
	mu          sync.RWMutex
}

// ShardedMap is a high-concurrency map implementation partitioned into 128 shards.
type ShardedMap [ShardCount]*shard

// NewShardedMap initializes a new ShardedMap by pre-allocating all 128 shards
// and their 64 sub-buckets (8,192 map instances total).
func NewShardedMap() *ShardedMap {
	var sm ShardedMap
	for i := range ShardCount {
		s := &shard{}
		for b := range SubBucketCount {
			s.buckets[b] = make(map[kv.Key]kv.Value)
		}
		sm[i] = s
	}
	return &sm
}

func (sm *ShardedMap) getShardByHash(hash kv.HashKey) *shard {
	return sm[hash%ShardCount]
}

// Get retrieves only the raw byte payload for a key.
// If the key is missing or is a tombstone, it returns false.
func (sm *ShardedMap) Get(key kv.Key) ([]byte, bool) {
	return sm.loadData(key, security.HashFunc(key))
}

// GetRecord retrieves the full kv.Value metadata for a key.
func (sm *ShardedMap) GetRecord(key kv.Key) (kv.Value, bool) {
	return sm.load(key, security.HashFunc(key))
}

// Put updates a value in the map and maintains rolling digests.
func (sm *ShardedMap) Put(key kv.Key, val kv.Value) {
	sm.store(key, security.HashFunc(key), val)
}

// PutLWW updates a value using Last-Write-Wins conflict resolution.
func (sm *ShardedMap) PutLWW(key kv.Key, val kv.Value) bool {
	return sm.storeLWW(key, security.HashFunc(key), val)
}

// Delete removes a key from the map and updates rolling digests.
func (sm *ShardedMap) Delete(key kv.Key) {
	sm.deleteKey(key, security.HashFunc(key))
}

func (sm *ShardedMap) load(key kv.Key, hash kv.HashKey) (kv.Value, bool) {
	shard := sm.getShardByHash(hash)
	subIndex := (hash >> 16) % SubBucketCount

	shard.mu.RLock()
	val, ok := shard.buckets[subIndex][key]
	shard.mu.RUnlock()

	if !ok {
		return kv.Value{}, false
	}
	val.ItemHash = 0 // Clear internal-only ItemHash to preserve DeepEqual assertions in tests
	return val, true
}

func (sm *ShardedMap) loadData(key kv.Key, hash kv.HashKey) ([]byte, bool) {
	shard := sm.getShardByHash(hash)
	subIndex := (hash >> 16) % SubBucketCount

	shard.mu.RLock()
	val, ok := shard.buckets[subIndex][key]
	shard.mu.RUnlock()

	if !ok || val.Tombstone {
		return nil, false
	}
	return val.Data, true
}

func getItemHash(hash kv.HashKey, val kv.Value) uint64 {
	// #nosec G115
	h := hash ^ bits.RotateLeft64(uint64(val.Timestamp), 17)

	if val.NodeID != "" {
		h ^= bits.RotateLeft64(security.HashFunc(val.NodeID), 31)
	}

	if len(val.Data) > 0 {
		h ^= bits.RotateLeft64(security.HashBytes(val.Data), 47)
	}

	if val.Tombstone {
		h ^= 0x5555555555555555
	}

	return h
}

func mixCount(count uint32) uint64 {
	x := uint64(count) + 1
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// Ordering is the tuple (Timestamp, NodeID, ItemHash).
func lwwWins(existing, incoming kv.Value) bool {
	if existing.Timestamp != incoming.Timestamp {
		return incoming.Timestamp > existing.Timestamp
	}
	if existing.NodeID != incoming.NodeID {
		return incoming.NodeID > existing.NodeID
	}
	return incoming.ItemHash > existing.ItemHash
}

func (sm *ShardedMap) store(key kv.Key, hash kv.HashKey, val kv.Value) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subIndex := (hash >> 16) % SubBucketCount
	bucket := shard.buckets[subIndex]

	existing, ok := bucket[key]
	if ok {
		oldItemHash := existing.ItemHash
		shard.subDigests[subIndex] ^= oldItemHash
		shard.shardDigest ^= oldItemHash
	} else {
		shard.subCounts[subIndex]++
		shard.shardCount++
	}

	val.ItemHash = getItemHash(hash, val)
	shard.subDigests[subIndex] ^= val.ItemHash
	shard.shardDigest ^= val.ItemHash

	bucket[key] = val
}

func (sm *ShardedMap) storeLWW(key kv.Key, hash kv.HashKey, val kv.Value) bool {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subIndex := (hash >> 16) % SubBucketCount
	bucket := shard.buckets[subIndex]

	val.ItemHash = getItemHash(hash, val)

	existing, ok := bucket[key]
	if ok {
		if !lwwWins(existing, val) {
			return false
		}
		oldItemHash := existing.ItemHash
		shard.subDigests[subIndex] ^= oldItemHash
		shard.shardDigest ^= oldItemHash
	} else {
		shard.subCounts[subIndex]++
		shard.shardCount++
	}

	shard.subDigests[subIndex] ^= val.ItemHash
	shard.shardDigest ^= val.ItemHash

	bucket[key] = val
	return true
}

func (sm *ShardedMap) deleteKey(key kv.Key, hash kv.HashKey) {
	shard := sm.getShardByHash(hash)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	subIndex := (hash >> 16) % SubBucketCount
	bucket := shard.buckets[subIndex]

	existing, ok := bucket[key]
	if ok {
		itemHash := existing.ItemHash
		shard.subDigests[subIndex] ^= itemHash
		shard.shardDigest ^= itemHash
		delete(bucket, key)
		shard.subCounts[subIndex]--
		shard.shardCount--
	}
}

// FillShardDigests populates the provided map with all shard IDs and their single intermediate XOR digests.
func (sm *ShardedMap) FillShardDigests(dst map[ShardID]Digest) {
	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		dst[ShardID(i)] = shard.shardDigest ^ mixCount(shard.shardCount)
		shard.mu.RUnlock()
	}
}

// RootDigest returns a single XOR hash of the entire database state.
func (sm *ShardedMap) RootDigest() RootDigest {
	var root RootDigest
	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		root ^= shard.shardDigest ^ mixCount(shard.shardCount)
		shard.mu.RUnlock()
	}
	return root
}

// FillDigests populates the provided map with all shard IDs and their current sub-bucket hashes.
func (sm *ShardedMap) FillDigests(dst map[ShardID]ShardDigest) {
	if dst == nil {
		return
	}
	for i := range ShardCount {
		id := ShardID(i)
		buf := dst[id]
		if len(buf) < SubBucketCount {
			buf = make(ShardDigest, SubBucketCount)
			dst[id] = buf
		}

		shard := sm[i]
		shard.mu.RLock()
		for b := range SubBucketCount {
			buf[b] = shard.subDigests[b] ^ mixCount(shard.subCounts[b])
		}
		shard.mu.RUnlock()
	}
}

type mapEntry struct {
	key kv.Key
	val kv.Value
}

// Range invokes the callback for each key-value pair in the map.
func (sm *ShardedMap) Range(callback func(key kv.Key, val kv.Value) bool) {
	for i := range ShardCount {
		shard := sm[i]
		shard.mu.RLock()
		if shard.shardCount == 0 {
			shard.mu.RUnlock()
			continue
		}
		entries := make([]mapEntry, 0, shard.shardCount)
		for b := range SubBucketCount {
			for k, v := range shard.buckets[b] {
				entries = append(entries, mapEntry{key: k, val: v})
			}
		}
		shard.mu.RUnlock()

		for _, e := range entries {
			if !callback(e.key, e.val) {
				return
			}
		}
	}
}

// RangeShard invokes the callback for each key-value pair in mismatched sub-buckets of a specific shard.
func (sm *ShardedMap) RangeShard(shardID ShardID, mismatchMask uint64, callback func(key kv.Key, val kv.Value)) {
	if shardID < 0 || shardID >= ShardCount {
		return
	}
	shard := sm[shardID]
	shard.mu.RLock()
	var entries []mapEntry
	for b := range SubBucketCount {
		if (mismatchMask & (1 << b)) != 0 {
			for k, v := range shard.buckets[b] {
				entries = append(entries, mapEntry{key: k, val: v})
			}
		}
	}
	shard.mu.RUnlock()

	for _, e := range entries {
		callback(e.key, e.val)
	}
}
