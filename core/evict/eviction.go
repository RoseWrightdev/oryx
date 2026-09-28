// Package evict provides cache eviction algorithms and helpers.
package evict

import (
	"context"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rosewrightdev/oryx/kv"
)

// Reason defines the category/reason for cache eviction (e.g. TTL or Capacity).
type Reason int

const (
	// ReasonTTL indicates the entry expired based on its lifespan.
	ReasonTTL Reason = iota
	// ReasonCapacity indicates the entry was evicted to free up memory due to reaching limit.
	ReasonCapacity
)

// Evictor defines the interface for cache invalidation.
type Evictor interface {
	Publish(key kv.Key, hash kv.HashKey)
	PublishDelete(key kv.Key, hash kv.HashKey)
	Start()
	Stop()
	SetEvictCallback(func(kv.Key, Reason) error)
}

type entry struct {
	expiry time.Time
	prev   *entry
	next   *entry
	key    string
	hash   kv.HashKey
}

type lruMsg struct {
	key  string
	hash kv.HashKey
}

type evictMsg struct {
	key    string
	reason Reason
}

type lruShard struct {
	ctx      context.Context
	tail     *entry
	ch       chan lruMsg
	delCh    chan kv.HashKey
	evictCh  chan evictMsg
	head     *entry
	cancel   context.CancelFunc
	cache    map[kv.HashKey]*entry
	onEvict  atomic.Pointer[func(kv.Key, Reason) error]
	pool     *sync.Pool
	wg       sync.WaitGroup
	ttl      time.Duration
	mu       sync.Mutex
	capacity uint32
}

// LeastRecentlyUsed implements a thread-safe, sharded Least Recently Used (LRU) eviction service.
type LeastRecentlyUsed struct {
	shards []*lruShard
	count  int
}

// LRUConfig defines the configuration parameters for the LeastRecentlyUsed eviction service.
type LRUConfig struct {
	Capacity   uint32
	TTL        time.Duration
	ShardCount int
}

// NewLRU creates and initializes a new LeastRecentlyUsed eviction service.
func NewLRU(config LRUConfig) *LeastRecentlyUsed {
	if config.Capacity == 0 {
		panic("oryx: LRU Capacity must be greater than 0")
	}
	shardCount := config.ShardCount
	if shardCount <= 0 {
		panic("oryx: LRU ShardCount must be greater than 0")
	}
	if shardCount > int(config.Capacity) {
		shardCount = int(config.Capacity)
	}
	shards := make([]*lruShard, shardCount)

	// #nosec G115
	shardCountU := uint32(shardCount)

	// Distribute capacity across shards
	shardCap := config.Capacity / shardCountU

	pool := &sync.Pool{
		New: func() any {
			return &entry{}
		},
	}

	for i := range shardCount {
		ctx, cancel := context.WithCancel(context.Background())
		shards[i] = &lruShard{
			ctx:      ctx,
			cancel:   cancel,
			ch:       make(chan lruMsg, max(shardCap/2, 8192)),
			delCh:    make(chan uint64, max(shardCap/2, 8192)),
			evictCh:  make(chan evictMsg, max(shardCap/4, 4096)),
			capacity: shardCap,
			cache:    make(map[kv.HashKey]*entry),
			ttl:      config.TTL,
			pool:     pool,
		}
	}

	return &LeastRecentlyUsed{
		shards: shards,
		count:  shardCount,
	}
}

// Start starts the background eviction loops for all shards.
func (lru *LeastRecentlyUsed) Start() {
	for _, s := range lru.shards {
		s.Start()
	}
}

func (s *lruShard) Start() {
	s.wg.Add(1)
	go s.run()
}

// Stop gracefully stops the background eviction loops for all shards.
func (lru *LeastRecentlyUsed) Stop() {
	for _, s := range lru.shards {
		s.Stop()
	}
}

func (s *lruShard) Stop() {
	s.cancel()
	s.wg.Wait()
}

// Publish sends a cache access telemetry event to the eviction queue.
func (lru *LeastRecentlyUsed) Publish(key kv.Key, hash kv.HashKey) {
	// Fast-path filter: immediately discard 50% of telemetry events
	// before executing any memory accesses, sharding, or queue checks.
	// #nosec G404
	if rand.Uint32()&1 != 0 {
		return
	}

	shard := lru.getShardByHash(hash)
	if !shouldSample(len(shard.ch), cap(shard.ch)) {
		return
	}

	// Clone the key synchronously, before Publish returns, so the caller is
	// free to recycle/overwrite the buffer it built key from (e.g. a gnet
	// socket read buffer). The run() goroutine that eventually dequeues this
	// message may not do so until long after Publish returns, by which point
	// the original buffer can already have been reused (#79).
	key = kv.Key(strings.Clone(string(key)))

	select {
	case shard.ch <- lruMsg{key: key, hash: hash}:
	default:
	}
}

func shouldSample(qLen, qCap int) bool {
	// Bypass completely if the channel is >= 80% full to eliminate lock contention.
	if qLen*5 >= qCap*4 {
		return false
	}

	// todo: add configuration options
	// 10-Tier Dynamic Exponential-Decay Sampling Tiers:
	// - < 1% full   : Sample 1-in-2    (mask 0, always publish remaining 50%)
	// - 1% - 5%     : Sample 1-in-4    (mask 1, discard 50% of remaining)
	// - 5% - 10%    : Sample 1-in-8    (mask 3, discard 75% of remaining)
	// - 10% - 20%   : Sample 1-in-16   (mask 7, discard 87.5% of remaining)
	// - 20% - 30%   : Sample 1-in-32   (mask 15)
	// - 30% - 40%   : Sample 1-in-64   (mask 31)
	// - 40% - 50%   : Sample 1-in-128  (mask 63)
	// - 50% - 60%   : Sample 1-in-256  (mask 127)
	// - 60% - 70%   : Sample 1-in-512  (mask 255)
	// - 70% - 80%   : Sample 1-in-1024 (mask 511)
	var mask uint32
	switch {
	case qLen*100 < qCap:
		mask = 0
	case qLen*20 < qCap:
		mask = 1
	case qLen*10 < qCap:
		mask = 3
	case qLen*5 < qCap:
		mask = 7
	case qLen*10 < qCap*3:
		mask = 15
	case qLen*5 < qCap*2:
		mask = 31
	case qLen*2 < qCap:
		mask = 63
	case qLen*5 < qCap*3:
		mask = 127
	case qLen*10 < qCap*7:
		mask = 255
	default:
		mask = 511
	}

	// #nosec G404
	return mask == 0 || rand.Uint32()&mask == 0
}

func (lru *LeastRecentlyUsed) seen(key kv.Key, hash kv.HashKey) {
	lru.getShardByHash(hash).seen(key, hash)
}

// PublishDelete sends a cache deletion telemetry event to the eviction queue.
func (lru *LeastRecentlyUsed) PublishDelete(_ kv.Key, hash kv.HashKey) {
	shard := lru.getShardByHash(hash)
	select {
	case shard.delCh <- hash:
	default:
	}
}

func (lru *LeastRecentlyUsed) getShardByHash(hash kv.HashKey) *lruShard {
	// #nosec G115
	countU := uint64(lru.count)
	idx := hash % countU
	// #nosec G115
	return lru.shards[int(idx)]
}

func (s *lruShard) run() {
	defer s.wg.Done()

	// When TTL is zero the caller has disabled time-based expiry.
	// time.NewTicker panics on a zero or negative duration, so we
	// only create the ticker when TTL is configured.
	var ticker *time.Ticker
	var tickerC <-chan time.Time
	if s.ttl > 0 {
		ticker = time.NewTicker(s.ttl / 10)
		defer ticker.Stop()
		tickerC = ticker.C
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		case msg := <-s.ch:
			s.seen(msg.key, msg.hash)
		case hKey := <-s.delCh:
			s.delete(hKey)
		case msg := <-s.evictCh:
			if fn := s.onEvict.Load(); fn != nil {
				_ = (*fn)(msg.key, msg.reason)
			}
		case <-tickerC:
			s.evictExpired()
		}
	}
}

func (s *lruShard) seen(key string, hkey kv.HashKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	// #nosec G404
	jitter := time.Duration(rand.Int64N(int64(s.ttl / 10)))
	expiry := now.Add(s.ttl + jitter)

	if e, ok := s.cache[hkey]; ok {
		e.expiry = expiry
		s.remove(e)
		s.pushFront(e)
		return
	}

	// #nosec G115
	if uint32(len(s.cache)) >= s.capacity {
		s.evictOldest()
	}

	e := s.pool.Get().(*entry)
	// key is already an owned, cloned string by the time it reaches seen():
	// Publish() clones it synchronously before enqueueing (#79), so it's safe
	// to store the alias here without a second copy.
	e.key = key
	e.hash = hkey
	e.expiry = expiry
	s.cache[hkey] = e
	s.pushFront(e)
}

func (s *lruShard) evictOldest() {
	if s.tail == nil {
		return
	}
	e := s.tail
	s.remove(e)
	delete(s.cache, e.hash)

	// Block until the eviction callback goroutine consumes the message.
	// A dropped eviction notification means the engine never receives the
	// callback, so the key leaks in storage memory forever (#65).
	s.evictCh <- evictMsg{key: e.key, reason: ReasonCapacity}

	e.key = ""
	s.pool.Put(e)
}

func (s *lruShard) evictExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for s.tail != nil {
		e := s.tail
		if e.expiry.After(now) {
			break
		}

		s.remove(e)
		delete(s.cache, e.hash)

		select {
		case s.evictCh <- evictMsg{key: e.key, reason: ReasonTTL}:
		default:
		}

		e.key = ""
		s.pool.Put(e)
	}
}

func (s *lruShard) delete(hKey kv.HashKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.cache[hKey]; ok {
		s.remove(e)
		delete(s.cache, hKey)
		e.key = ""
		s.pool.Put(e)
	}
}

func (s *lruShard) remove(e *entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (s *lruShard) pushFront(e *entry) {
	e.next = s.head
	e.prev = nil
	if s.head != nil {
		s.head.prev = e
	}
	s.head = e
	if s.tail == nil {
		s.tail = e
	}
}

// SetEvictCallback sets the function to be called when an entry is evicted.
func (lru *LeastRecentlyUsed) SetEvictCallback(fn func(kv.Key, Reason) error) {
	for _, s := range lru.shards {
		s.onEvict.Store(&fn)
	}
}

// GetShardTTL returns the TTL of the shard at the given index.
func (lru *LeastRecentlyUsed) GetShardTTL(idx int) time.Duration {
	return lru.shards[idx].ttl
}

// GetShardCapacity returns the capacity of the shard at the given index.
func (lru *LeastRecentlyUsed) GetShardCapacity(idx int) uint32 {
	return lru.shards[idx].capacity
}

// Occupancy returns the ratio of cached entries to total capacity across all shards.
func (lru *LeastRecentlyUsed) Occupancy() float64 {
	var totalSize uint32
	var totalCap uint32
	for _, s := range lru.shards {
		s.mu.Lock()
		// #nosec G115
		totalSize += uint32(len(s.cache))
		totalCap += s.capacity
		s.mu.Unlock()
	}
	if totalCap == 0 {
		return 0
	}
	return float64(totalSize) / float64(totalCap)
}
