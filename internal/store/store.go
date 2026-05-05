package store

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrNotFound            = errors.New("key not found")
	ErrNotInteger          = errors.New("value is not an unsigned integer")
	ErrOverflow            = errors.New("increment would overflow uint64")
	ErrValueTooLarge       = errors.New("value too large")
	ErrMemoryLimitExceeded = errors.New("memory limit exceeded")
	ErrCasMismatch         = errors.New("cas version does not match")
)

const DefaultShards = 16

type Config struct {
	MaxValueBytes     int
	MaxMemoryBytes    int
	ItemOverheadBytes int
	Shards            int
	Now               func() time.Time
}

type Item struct {
	Value []byte
	// CAS is a monotonic per-store version stamped on every successful
	// mutation. Pass it to Cas to perform an optimistic-concurrency
	// write that only succeeds when the value hasn't changed since the
	// caller observed it.
	CAS uint64
}

type storedItem struct {
	key       string
	value     []byte
	expiresAt time.Time
	size      int
	cas       uint64
	prev      *storedItem
	next      *storedItem
}

// Store is a sharded key-value cache. Keys are hashed to one of
// config.Shards independently-locked shards; each shard owns its own
// LRU and memory budget. Public methods are concurrency-safe.
//
// The cas counter is process-global and monotonic. Each successful
// mutation (Set, Incr, Cas) increments it atomically and stamps the
// resulting value with the new version. Tokens are unique across all
// keys and the lifetime of the process.
type Store struct {
	shards []*shard
	cas    atomic.Uint64
}

func DefaultConfig() Config {
	return Config{
		MaxValueBytes:     1024 * 1024,
		MaxMemoryBytes:    64 * 1024 * 1024,
		ItemOverheadBytes: 64,
		Shards:            DefaultShards,
		Now:               time.Now,
	}
}

func New(config Config) *Store {
	config = normalizeConfig(config)
	perShardMemory := config.MaxMemoryBytes / config.Shards
	if perShardMemory < 1 {
		perShardMemory = 1
	}
	s := &Store{}
	s.shards = make([]*shard, config.Shards)
	for i := range s.shards {
		s.shards[i] = newShard(config, perShardMemory, &s.cas)
	}
	return s
}

// Set stores value under key with the given TTL.
//
// The store takes ownership of value's underlying bytes. Callers must not
// mutate the slice after Set returns. Get returns a private copy, so callers
// may safely mutate slices returned from Get.
func (s *Store) Set(key string, value []byte, ttl time.Duration) error {
	return s.shardFor(key).set(key, value, ttl)
}

func (s *Store) Get(key string) (Item, bool) {
	return s.shardFor(key).get(key)
}

func (s *Store) Delete(key string) bool {
	return s.shardFor(key).delete(key)
}

func (s *Store) Incr(key string, delta uint64) (uint64, error) {
	return s.shardFor(key).incr(key, delta)
}

// Cas writes value under key only when expected matches the value's
// current CAS version. Returns ErrNotFound if the key is absent or
// expired, and ErrCasMismatch if the key exists but its version is
// different.
func (s *Store) Cas(key string, value []byte, ttl time.Duration, expected uint64) error {
	return s.shardFor(key).cas(key, value, ttl, expected)
}

func (s *Store) CleanupExpired() int {
	total := 0
	for _, sh := range s.shards {
		total += sh.cleanupExpired()
	}
	return total
}

func (s *Store) MemoryBytes() int {
	total := 0
	for _, sh := range s.shards {
		total += sh.memoryBytesSnapshot()
	}
	return total
}

// Stats is an operator-facing snapshot of store-wide counters. Values are
// best-effort: each shard is sampled independently, so a snapshot taken
// during heavy traffic may show counters from slightly different points
// in time. The numbers are still useful for trend-watching.
type Stats struct {
	Items           int      // total live items across all shards
	MemoryBytes     int      // accounted bytes across all shards
	MaxMemoryBytes  int      // configured budget across all shards
	Evictions       uint64
	Expirations     uint64
	ItemsPerShard   []int    // one entry per shard, in shard-index order
	NumShards       int
}

// Stats samples per-shard counters and returns an aggregated snapshot.
func (s *Store) Stats() Stats {
	out := Stats{
		NumShards:     len(s.shards),
		ItemsPerShard: make([]int, len(s.shards)),
	}
	for i, sh := range s.shards {
		sh.mu.Lock()
		out.Items += len(sh.items)
		out.MemoryBytes += sh.memoryBytes
		out.MaxMemoryBytes += sh.maxMemoryBytes
		out.ItemsPerShard[i] = len(sh.items)
		sh.mu.Unlock()
		out.Evictions += sh.evictions.Load()
		out.Expirations += sh.expirations.Load()
	}
	return out
}

func (s *Store) shardFor(key string) *shard {
	return s.shards[shardIndex(key, len(s.shards))]
}

// shardIndex hashes key with FNV-1a-32 and reduces to [0, n). Inlined
// rather than using hash/fnv's allocating constructors so the hot path
// is allocation-free.
func shardIndex(key string, n int) uint32 {
	const (
		offset uint32 = 2166136261
		prime  uint32 = 16777619
	)
	h := offset
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime
	}
	return h % uint32(n)
}

func normalizeConfig(config Config) Config {
	defaults := DefaultConfig()
	if config.MaxValueBytes <= 0 {
		config.MaxValueBytes = defaults.MaxValueBytes
	}
	if config.MaxMemoryBytes <= 0 {
		config.MaxMemoryBytes = defaults.MaxMemoryBytes
	}
	if config.ItemOverheadBytes < 0 {
		config.ItemOverheadBytes = defaults.ItemOverheadBytes
	}
	if config.Shards <= 0 {
		config.Shards = defaults.Shards
	}
	if config.Now == nil {
		config.Now = defaults.Now
	}
	return config
}

func expiresAt(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

func (i storedItem) expired(now time.Time) bool {
	return !i.expiresAt.IsZero() && !now.Before(i.expiresAt)
}

func cloneBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}

func isDecimal(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	for _, b := range value {
		if b < '0' || b > '9' {
			return false
		}
	}
	return true
}
