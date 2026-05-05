package store

import (
	"errors"
	"testing"
	"time"
)

func TestStoreInsulatesGetCallersFromEachOther(t *testing.T) {
	// Set takes ownership of the caller's bytes (callers must not mutate
	// after Set returns). Get returns a fresh copy each time, so mutating
	// the slice returned from one Get must not affect the next.
	s := New(DefaultConfig())

	if err := s.Set("greeting", []byte("hello"), 0); err != nil {
		t.Fatalf("set greeting: %v", err)
	}

	got, ok := s.Get("greeting")
	if !ok {
		t.Fatal("expected stored key to be found")
	}
	got.Value[0] = 'y'

	again, ok := s.Get("greeting")
	if !ok {
		t.Fatal("expected stored key to still be found")
	}
	if string(again.Value) != "hello" {
		t.Fatalf("stored value changed through returned slice: %q", again.Value)
	}
}

func TestStoreDeleteReportsWhetherKeyExisted(t *testing.T) {
	s := New(DefaultConfig())
	if err := s.Set("session", []byte("abc"), 0); err != nil {
		t.Fatalf("set session: %v", err)
	}

	if !s.Delete("session") {
		t.Fatal("expected delete to report an existing key")
	}
	if _, ok := s.Get("session"); ok {
		t.Fatal("expected deleted key to be absent")
	}
	if s.Delete("session") {
		t.Fatal("expected second delete to report a missing key")
	}
}

func TestStoreIncrUpdatesDecimalBlob(t *testing.T) {
	s := New(DefaultConfig())
	if err := s.Set("counter", []byte("41"), 0); err != nil {
		t.Fatalf("set counter: %v", err)
	}

	value, err := s.Incr("counter", 1)
	if err != nil {
		t.Fatalf("expected incr to succeed: %v", err)
	}
	if value != 42 {
		t.Fatalf("expected new counter value 42, got %d", value)
	}

	got, ok := s.Get("counter")
	if !ok {
		t.Fatal("expected counter to remain stored")
	}
	if string(got.Value) != "42" {
		t.Fatalf("expected counter blob to be rewritten, got %q", got.Value)
	}
}

func TestStoreIncrReturnsDeterministicErrors(t *testing.T) {
	s := New(DefaultConfig())

	if _, err := s.Incr("missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing key, got %v", err)
	}

	if err := s.Set("name", []byte("emre"), 0); err != nil {
		t.Fatalf("set name: %v", err)
	}
	if _, err := s.Incr("name", 1); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("expected ErrNotInteger for non-decimal blob, got %v", err)
	}
}

func TestStoreTreatsExpiredKeysAsMissingAndCleansAccounting(t *testing.T) {
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	s := New(Config{
		MaxValueBytes:     128,
		MaxMemoryBytes:    1024,
		ItemOverheadBytes: 16,
		Now: func() time.Time {
			return now
		},
	})

	if err := s.Set("token", []byte("abc"), time.Second); err != nil {
		t.Fatalf("set token: %v", err)
	}
	if got := s.MemoryBytes(); got != len("token")+len("abc")+16 {
		t.Fatalf("expected accounted bytes for live item, got %d", got)
	}

	now = now.Add(2 * time.Second)
	if _, ok := s.Get("token"); ok {
		t.Fatal("expected expired key to be invisible to get")
	}
	if _, err := s.Incr("token", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected expired key to be missing for incr, got %v", err)
	}
	if s.Delete("token") {
		t.Fatal("expected expired key to be missing for delete")
	}
	if got := s.MemoryBytes(); got != 0 {
		t.Fatalf("expected lazy expiry to release accounted bytes, got %d", got)
	}
}

func TestStoreRejectsValuesTooLargeAndItemsTooLargeForMemoryLimit(t *testing.T) {
	s := New(Config{
		MaxValueBytes:     4,
		MaxMemoryBytes:    4,
		ItemOverheadBytes: 0,
		Shards:            1,
	})

	if err := s.Set("too-big", []byte("12345"), 0); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got %v", err)
	}
	if err := s.Set("a", []byte("123"), 0); err != nil {
		t.Fatalf("expected first value to fit exactly: %v", err)
	}
	if err := s.Set("too-wide", []byte("1"), 0); !errors.Is(err, ErrMemoryLimitExceeded) {
		t.Fatalf("expected ErrMemoryLimitExceeded, got %v", err)
	}
	if got := s.MemoryBytes(); got != len("a")+3 {
		t.Fatalf("expected failed write not to change accounting, got %d", got)
	}
}

func TestStoreEvictsLeastRecentlyUsedItemWhenSetExceedsMemoryLimit(t *testing.T) {
	s := New(Config{
		MaxValueBytes:     16,
		MaxMemoryBytes:    6,
		ItemOverheadBytes: 0,
		Shards:            1,
	})

	if err := s.Set("a", []byte("11"), 0); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := s.Set("b", []byte("22"), 0); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if _, ok := s.Get("a"); !ok {
		t.Fatal("expected get to find a")
	}
	if err := s.Set("c", []byte("33"), 0); err != nil {
		t.Fatalf("set c should evict b: %v", err)
	}

	if _, ok := s.Get("b"); ok {
		t.Fatal("expected least recently used key b to be evicted")
	}
	if _, ok := s.Get("a"); !ok {
		t.Fatal("expected recently read key a to remain")
	}
	if _, ok := s.Get("c"); !ok {
		t.Fatal("expected new key c to be stored")
	}
	if got := s.MemoryBytes(); got != 6 {
		t.Fatalf("expected accounting for two stored items, got %d", got)
	}
}

func TestStoreRemovesExpiredItemsBeforeEvictingLiveItems(t *testing.T) {
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	s := New(Config{
		MaxValueBytes:     16,
		MaxMemoryBytes:    6,
		ItemOverheadBytes: 0,
		Shards:            1,
		Now: func() time.Time {
			return now
		},
	})

	if err := s.Set("a", []byte("11"), time.Second); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := s.Set("b", []byte("22"), 0); err != nil {
		t.Fatalf("set b: %v", err)
	}

	now = now.Add(2 * time.Second)
	if err := s.Set("c", []byte("33"), 0); err != nil {
		t.Fatalf("set c should reuse expired space: %v", err)
	}

	if _, ok := s.Get("a"); ok {
		t.Fatal("expected expired key a to be gone")
	}
	if _, ok := s.Get("b"); !ok {
		t.Fatal("expected live key b to remain")
	}
	if _, ok := s.Get("c"); !ok {
		t.Fatal("expected new key c to be stored")
	}
	if got := s.MemoryBytes(); got != 6 {
		t.Fatalf("expected accounting to exclude expired item, got %d", got)
	}
}

func TestStoreIncrRefreshesRecencyForEviction(t *testing.T) {
	s := New(Config{
		MaxValueBytes:     16,
		MaxMemoryBytes:    6,
		ItemOverheadBytes: 0,
		Shards:            1,
	})

	if err := s.Set("a", []byte("1"), 0); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := s.Set("b", []byte("22"), 0); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if _, err := s.Incr("a", 1); err != nil {
		t.Fatalf("incr a: %v", err)
	}
	if err := s.Set("c", []byte("33"), 0); err != nil {
		t.Fatalf("set c should evict b: %v", err)
	}

	if _, ok := s.Get("b"); ok {
		t.Fatal("expected b to be evicted after incr refreshed a")
	}
	if got, ok := s.Get("a"); !ok || string(got.Value) != "2" {
		t.Fatalf("expected incremented a to remain as 2, got %q found=%v", got.Value, ok)
	}
}

func TestStoreDeleteRemovesAccountingBeforeLaterEviction(t *testing.T) {
	s := New(Config{
		MaxValueBytes:     16,
		MaxMemoryBytes:    6,
		ItemOverheadBytes: 0,
		Shards:            1,
	})

	if err := s.Set("a", []byte("11"), 0); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := s.Set("b", []byte("22"), 0); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if !s.Delete("a") {
		t.Fatal("expected delete to remove a")
	}
	if err := s.Set("c", []byte("33"), 0); err != nil {
		t.Fatalf("set c should fit after delete: %v", err)
	}

	if _, ok := s.Get("b"); !ok {
		t.Fatal("expected b to remain")
	}
	if _, ok := s.Get("c"); !ok {
		t.Fatal("expected c to be stored")
	}
	if got := s.MemoryBytes(); got != 6 {
		t.Fatalf("expected accounting for b and c, got %d", got)
	}
}

func TestStoreSetDoesNotEvictKeyBeingUpdatedOnOverLimitFailure(t *testing.T) {
	s := New(Config{
		MaxValueBytes:     16,
		MaxMemoryBytes:    4,
		ItemOverheadBytes: 0,
		Shards:            1,
	})

	if err := s.Set("a", []byte("1"), 0); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := s.Set("a", []byte("1234"), 0); !errors.Is(err, ErrMemoryLimitExceeded) {
		t.Fatalf("expected ErrMemoryLimitExceeded, got %v", err)
	}

	got, ok := s.Get("a")
	if !ok {
		t.Fatal("expected old value to remain after failed update")
	}
	if string(got.Value) != "1" {
		t.Fatalf("expected old value to remain after failed update, got %q", got.Value)
	}
	if got := s.MemoryBytes(); got != 2 {
		t.Fatalf("expected failed update not to change accounting, got %d", got)
	}
}

func TestStoreCleanupExpiredRemovesOnlyExpiredItems(t *testing.T) {
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	s := New(Config{
		MaxValueBytes:  128,
		MaxMemoryBytes: 1024,
		Now: func() time.Time {
			return now
		},
	})

	if err := s.Set("short", []byte("1"), time.Second); err != nil {
		t.Fatalf("set short: %v", err)
	}
	if err := s.Set("forever", []byte("1"), 0); err != nil {
		t.Fatalf("set forever: %v", err)
	}

	now = now.Add(2 * time.Second)
	if removed := s.CleanupExpired(); removed != 1 {
		t.Fatalf("expected one expired item to be removed, got %d", removed)
	}
	if _, ok := s.Get("forever"); !ok {
		t.Fatal("expected non-expiring item to remain")
	}
}

func TestStoreSetStampsMonotonicCASAcrossWrites(t *testing.T) {
	s := New(DefaultConfig())
	if err := s.Set("a", []byte("1"), 0); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := s.Set("b", []byte("2"), 0); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if err := s.Set("a", []byte("3"), 0); err != nil {
		t.Fatalf("rewrite a: %v", err)
	}

	a, _ := s.Get("a")
	b, _ := s.Get("b")
	if a.CAS == 0 || b.CAS == 0 {
		t.Fatalf("CAS tokens must be non-zero, got a=%d b=%d", a.CAS, b.CAS)
	}
	if a.CAS <= b.CAS {
		t.Errorf("a's third write should produce a higher CAS than b: a=%d b=%d", a.CAS, b.CAS)
	}
}

func TestStoreCasReturnsErrNotFoundWhenAbsent(t *testing.T) {
	s := New(DefaultConfig())
	err := s.Cas("missing", []byte("v"), 0, 1)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Cas on missing key: got %v, want ErrNotFound", err)
	}
}

func TestStoreCasReturnsErrCasMismatchOnWrongVersion(t *testing.T) {
	s := New(DefaultConfig())
	if err := s.Set("counter", []byte("1"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, _ := s.Get("counter")

	err := s.Cas("counter", []byte("2"), 0, got.CAS+999)
	if !errors.Is(err, ErrCasMismatch) {
		t.Fatalf("Cas with wrong version: got %v, want ErrCasMismatch", err)
	}

	again, _ := s.Get("counter")
	if string(again.Value) != "1" || again.CAS != got.CAS {
		t.Errorf("Cas mismatch must not mutate: value=%q cas-before=%d cas-after=%d", again.Value, got.CAS, again.CAS)
	}
}

func TestStoreCasMatchUpdatesAndBumpsVersion(t *testing.T) {
	s := New(DefaultConfig())
	if err := s.Set("counter", []byte("1"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	first, _ := s.Get("counter")

	if err := s.Cas("counter", []byte("2"), 0, first.CAS); err != nil {
		t.Fatalf("Cas with right version: %v", err)
	}

	after, _ := s.Get("counter")
	if string(after.Value) != "2" {
		t.Errorf("value not updated: %q", after.Value)
	}
	if after.CAS <= first.CAS {
		t.Errorf("CAS should bump after successful Cas: before=%d after=%d", first.CAS, after.CAS)
	}
}
