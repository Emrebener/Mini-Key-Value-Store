package store

import (
	"container/list"
	"math"
	"strconv"
	"sync"
	"time"
)

// shard owns one slice of the store's keyspace. Each shard has its own
// mutex, items map, LRU list, and memory budget. The store fans out by
// hashing the key to a shard index.
//
// Sharding relaxes the global "exact LRU" guarantee from vol 1 to
// "exact LRU within a shard, approximate across the store." The
// alternative (a global LRU + memory counter shared across shards)
// would require cross-shard locks on every eviction and defeat the
// reason for sharding in the first place.
type shard struct {
	mu                sync.Mutex
	items             map[string]storedItem
	lru               *list.List
	memoryBytes       int
	ttlCount          int
	maxValueBytes     int
	maxMemoryBytes    int
	itemOverheadBytes int
	now               func() time.Time
}

func newShard(cfg Config, perShardMemory int) *shard {
	return &shard{
		items:             make(map[string]storedItem),
		lru:               list.New(),
		maxValueBytes:     cfg.MaxValueBytes,
		maxMemoryBytes:    perShardMemory,
		itemOverheadBytes: cfg.ItemOverheadBytes,
		now:               cfg.Now,
	}
}

func (sh *shard) set(key string, value []byte, ttl time.Duration) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if len(value) > sh.maxValueBytes {
		return ErrValueTooLarge
	}

	now := sh.now()
	oldSize := 0
	if old, ok := sh.items[key]; ok {
		if old.expired(now) {
			sh.deleteLocked(key, old)
		} else {
			oldSize = old.size
		}
	}

	size := sh.itemSize(key, len(value))
	if size > sh.maxMemoryBytes {
		return ErrMemoryLimitExceeded
	}
	projected := sh.memoryBytes - oldSize + size
	if projected > sh.maxMemoryBytes {
		if sh.ttlCount > 0 {
			sh.removeExpiredLocked(now)
			oldSize = 0
			if old, ok := sh.items[key]; ok {
				oldSize = old.size
			}
			projected = sh.memoryBytes - oldSize + size
		}
		if projected > sh.maxMemoryBytes {
			if !sh.evictUntilFitsLocked(key, projected) {
				return ErrMemoryLimitExceeded
			}
			projected = sh.memoryBytes - oldSize + size
		}
	}

	if old, ok := sh.items[key]; ok {
		sh.lru.Remove(old.element)
	}
	element := sh.lru.PushFront(key)
	expiry := expiresAt(now, ttl)
	sh.items[key] = storedItem{
		value:     value,
		expiresAt: expiry,
		size:      size,
		element:   element,
	}
	sh.memoryBytes = projected
	if !expiry.IsZero() {
		sh.ttlCount++
	}
	return nil
}

func (sh *shard) get(key string) (Item, bool) {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	item, ok := sh.items[key]
	if !ok {
		return Item{}, false
	}
	if item.expired(sh.now()) {
		sh.deleteLocked(key, item)
		return Item{}, false
	}
	sh.lru.MoveToFront(item.element)
	return Item{Value: cloneBytes(item.value)}, true
}

func (sh *shard) delete(key string) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	item, ok := sh.items[key]
	if !ok {
		return false
	}
	if item.expired(sh.now()) {
		sh.deleteLocked(key, item)
		return false
	}
	sh.deleteLocked(key, item)
	return true
}

func (sh *shard) incr(key string, delta uint64) (uint64, error) {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	item, ok := sh.items[key]
	if !ok {
		return 0, ErrNotFound
	}
	now := sh.now()
	if item.expired(now) {
		sh.deleteLocked(key, item)
		return 0, ErrNotFound
	}
	if !isDecimal(item.value) {
		return 0, ErrNotInteger
	}

	current, err := strconv.ParseUint(string(item.value), 10, 64)
	if err != nil {
		return 0, ErrNotInteger
	}
	if math.MaxUint64-current < delta {
		return 0, ErrOverflow
	}

	next := current + delta
	nextValue := []byte(strconv.FormatUint(next, 10))
	if len(nextValue) > sh.maxValueBytes {
		return 0, ErrValueTooLarge
	}
	nextSize := sh.itemSize(key, len(nextValue))
	if nextSize > sh.maxMemoryBytes {
		return 0, ErrMemoryLimitExceeded
	}
	projected := sh.memoryBytes - item.size + nextSize
	if projected > sh.maxMemoryBytes {
		if sh.ttlCount > 0 {
			sh.removeExpiredLocked(now)
			item, ok = sh.items[key]
			if !ok {
				return 0, ErrNotFound
			}
			projected = sh.memoryBytes - item.size + nextSize
		}
		if projected > sh.maxMemoryBytes && !sh.evictUntilFitsLocked(key, projected) {
			return 0, ErrMemoryLimitExceeded
		}
		projected = sh.memoryBytes - item.size + nextSize
	}

	expiry := item.expiresAt
	sh.items[key] = storedItem{
		value:     nextValue,
		expiresAt: expiry,
		size:      nextSize,
		element:   item.element,
	}
	sh.lru.MoveToFront(item.element)
	sh.memoryBytes = projected
	return next, nil
}

func (sh *shard) cleanupExpired() int {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := sh.now()
	removed := 0
	for key, item := range sh.items {
		if item.expired(now) {
			sh.deleteLocked(key, item)
			removed++
		}
	}
	return removed
}

func (sh *shard) memoryBytesSnapshot() int {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.memoryBytes
}

func (sh *shard) removeExpiredLocked(now time.Time) {
	for key, item := range sh.items {
		if item.expired(now) {
			sh.deleteLocked(key, item)
		}
	}
}

func (sh *shard) evictUntilFitsLocked(protectedKey string, projected int) bool {
	for projected > sh.maxMemoryBytes {
		element := sh.lru.Back()
		for element != nil && element.Value.(string) == protectedKey {
			element = element.Prev()
		}
		if element == nil {
			return false
		}

		key := element.Value.(string)
		item := sh.items[key]
		sh.deleteLocked(key, item)
		projected -= item.size
	}
	return true
}

func (sh *shard) deleteLocked(key string, item storedItem) {
	delete(sh.items, key)
	sh.lru.Remove(item.element)
	sh.memoryBytes -= item.size
	if !item.expiresAt.IsZero() {
		sh.ttlCount--
	}
}

func (sh *shard) itemSize(key string, valueBytes int) int {
	return len(key) + valueBytes + sh.itemOverheadBytes
}
