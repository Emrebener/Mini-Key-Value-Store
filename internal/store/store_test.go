package store

import (
	"errors"
	"testing"
)

func TestStoreCopiesValuesAcrossSetAndGet(t *testing.T) {
	s := New()
	input := []byte("hello")

	s.Set("greeting", input)
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
	s := New()
	s.Set("session", []byte("abc"))

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
	s := New()
	s.Set("counter", []byte("41"))

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
	s := New()

	if _, err := s.Incr("missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing key, got %v", err)
	}

	s.Set("name", []byte("emre"))
	if _, err := s.Incr("name", 1); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("expected ErrNotInteger for non-decimal blob, got %v", err)
	}
}
