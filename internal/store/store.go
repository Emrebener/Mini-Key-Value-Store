package store

import (
	"container/list"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNotFound            = errors.New("key not found")
	ErrNotInteger          = errors.New("value is not an unsigned integer")
	ErrOverflow            = errors.New("increment would overflow uint64")
	ErrValueTooLarge       = errors.New("value too large")
	ErrMemoryLimitExceeded = errors.New("memory limit exceeded")
)

type Config struct {
	MaxValueBytes     int
	MaxMemoryBytes    int
	ItemOverheadBytes int
	Now               func() time.Time
}

type Item struct {
	Value []byte
}

type storedItem struct {
	value     []byte
	expiresAt time.Time
	size      int
	element   *list.Element
}

type Store struct {
	mu          sync.Mutex
	items       map[string]storedItem
	lru         *list.List
	config      Config
	memoryBytes int
}

func DefaultConfig() Config {
	return Config{
		MaxValueBytes:     1024 * 1024,
		MaxMemoryBytes:    64 * 1024 * 1024,
		ItemOverheadBytes: 64,
		Now:               time.Now,
	}
}

func New(config Config) *Store {
	config = normalizeConfig(config)
	return &Store{
		items:  make(map[string]storedItem),
		lru:    list.New(),
		config: config,
	}
}

// Set stores value under key with the given TTL.
//
// The store takes ownership of value's underlying bytes. Callers must not
// mutate the slice after Set returns. Get returns a private copy, so callers
// may safely mutate slices returned from Get.
func (s *Store) Set(key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(value) > s.config.MaxValueBytes {
		return ErrValueTooLarge
	}

	now := s.now()
	oldSize := 0
	if old, ok := s.items[key]; ok {
		if old.expired(now) {
			s.deleteLocked(key, old)
		} else {
			oldSize = old.size
		}
	}

	size := s.itemSize(key, len(value))
	if size > s.config.MaxMemoryBytes {
		return ErrMemoryLimitExceeded
	}
	projected := s.memoryBytes - oldSize + size
	if projected > s.config.MaxMemoryBytes {
		s.removeExpiredLocked(now)
		oldSize = 0
		if old, ok := s.items[key]; ok {
			oldSize = old.size
		}
		projected = s.memoryBytes - oldSize + size
		if projected > s.config.MaxMemoryBytes {
			if !s.evictUntilFitsLocked(key, projected) {
				return ErrMemoryLimitExceeded
			}
			projected = s.memoryBytes - oldSize + size
		}
	}

	var element *list.Element
	if old, ok := s.items[key]; ok {
		s.lru.Remove(old.element)
	}
	element = s.lru.PushFront(key)
	s.items[key] = storedItem{
		value:     value,
		expiresAt: expiresAt(now, ttl),
		size:      size,
		element:   element,
	}
	s.memoryBytes = projected
	return nil
}

func (s *Store) Get(key string) (Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return Item{}, false
	}
	if item.expired(s.now()) {
		s.deleteLocked(key, item)
		return Item{}, false
	}
	s.lru.MoveToFront(item.element)
	return Item{Value: cloneBytes(item.value)}, true
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return false
	}
	if item.expired(s.now()) {
		s.deleteLocked(key, item)
		return false
	}
	s.deleteLocked(key, item)
	return true
}

func (s *Store) Incr(key string, delta uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return 0, ErrNotFound
	}
	now := s.now()
	if item.expired(now) {
		s.deleteLocked(key, item)
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
	if len(nextValue) > s.config.MaxValueBytes {
		return 0, ErrValueTooLarge
	}
	nextSize := s.itemSize(key, len(nextValue))
	if nextSize > s.config.MaxMemoryBytes {
		return 0, ErrMemoryLimitExceeded
	}
	projected := s.memoryBytes - item.size + nextSize
	if projected > s.config.MaxMemoryBytes {
		s.removeExpiredLocked(now)
		item, ok = s.items[key]
		if !ok {
			return 0, ErrNotFound
		}
		projected = s.memoryBytes - item.size + nextSize
		if projected > s.config.MaxMemoryBytes && !s.evictUntilFitsLocked(key, projected) {
			return 0, ErrMemoryLimitExceeded
		}
		projected = s.memoryBytes - item.size + nextSize
	}

	s.items[key] = storedItem{
		value:     nextValue,
		expiresAt: item.expiresAt,
		size:      nextSize,
		element:   item.element,
	}
	s.lru.MoveToFront(item.element)
	s.memoryBytes = projected
	return next, nil
}

func (s *Store) CleanupExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	removed := 0
	for key, item := range s.items {
		if item.expired(now) {
			s.deleteLocked(key, item)
			removed++
		}
	}
	return removed
}

func (s *Store) MemoryBytes() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.memoryBytes
}

func (s *Store) removeExpiredLocked(now time.Time) {
	for key, item := range s.items {
		if item.expired(now) {
			s.deleteLocked(key, item)
		}
	}
}

func (s *Store) evictUntilFitsLocked(protectedKey string, projected int) bool {
	for projected > s.config.MaxMemoryBytes {
		element := s.lru.Back()
		for element != nil && element.Value.(string) == protectedKey {
			element = element.Prev()
		}
		if element == nil {
			return false
		}

		key := element.Value.(string)
		item := s.items[key]
		s.deleteLocked(key, item)
		projected -= item.size
	}
	return true
}

func (s *Store) itemSize(key string, valueBytes int) int {
	return len(key) + valueBytes + s.config.ItemOverheadBytes
}

func (s *Store) now() time.Time {
	return s.config.Now()
}

func (s *Store) deleteLocked(key string, item storedItem) {
	delete(s.items, key)
	s.lru.Remove(item.element)
	s.memoryBytes -= item.size
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
