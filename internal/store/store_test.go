package store

import (
	"errors"
	"testing"
	"time"
)

func TestStoreCopiesValuesAcrossSetAndGet(t *testing.T) {
	s := New(DefaultConfig())
	input := []byte("hello")

	if err := s.Set("greeting", input, 0); err != nil {
		t.Fatalf("set greeting: %v", err)
	}
	input[0] = 'j'

	got, ok := s.Get("greeting")
	if !ok {
		t.Fatal("expected stored key to be found")
	}
	if string(got.Value) != "hello" {
		t.Fatalf("stored value changed through caller slice: %q", got.Value)
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

func TestStoreRejectsWritesOverConfiguredValueOrMemoryLimits(t *testing.T) {
	s := New(Config{
		MaxValueBytes:     4,
		MaxMemoryBytes:    len("a") + 4 + 8,
		ItemOverheadBytes: 8,
	})

	if err := s.Set("too-big", []byte("12345"), 0); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got %v", err)
	}
	if err := s.Set("a", []byte("1234"), 0); err != nil {
		t.Fatalf("expected first value to fit exactly: %v", err)
	}
	if err := s.Set("b", []byte("1"), 0); !errors.Is(err, ErrMemoryLimitExceeded) {
		t.Fatalf("expected ErrMemoryLimitExceeded, got %v", err)
	}
	if got := s.MemoryBytes(); got != len("a")+4+8 {
		t.Fatalf("expected failed write not to change accounting, got %d", got)
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
