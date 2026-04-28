package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
)

var (
	ErrNotFound   = errors.New("key not found")
	ErrNotInteger = errors.New("value is not an unsigned integer")
	ErrOverflow   = errors.New("increment would overflow uint64")
)

type Item struct {
	Value []byte
}

type Store struct {
	mu    sync.RWMutex
	items map[string]Item
}

func New() *Store {
	return &Store{items: make(map[string]Item)}
}

func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[key] = Item{Value: cloneBytes(value)}
}

func (s *Store) Get(key string) (Item, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, ok := s.items[key]
	if !ok {
		return Item{}, false
	}
	return Item{Value: cloneBytes(item.Value)}, true
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.items[key]; !ok {
		return false
	}
	delete(s.items, key)
	return true
}

func (s *Store) Incr(key string, delta uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return 0, ErrNotFound
	}
	if !isDecimal(item.Value) {
		return 0, ErrNotInteger
	}

	current, err := strconv.ParseUint(string(item.Value), 10, 64)
	if err != nil {
		return 0, ErrNotInteger
	}
	if math.MaxUint64-current < delta {
		return 0, ErrOverflow
	}

	next := current + delta
	s.items[key] = Item{Value: []byte(strconv.FormatUint(next, 10))}
	return next, nil
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
