package server

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Emrebener/Mini-Key-Value-Store/internal/store"
)

func serveAll(t *testing.T, s *Server, input string) string {
	t.Helper()
	var output bytes.Buffer
	if err := s.Serve(strings.NewReader(input), &output); err != nil {
		t.Fatalf("serve: %v", err)
	}
	return strings.TrimSuffix(output.String(), "\r\n")
}

func TestSessionExecutesProtocolCommands(t *testing.T) {
	s := New(store.New(store.DefaultConfig()))
	got := serveAll(t, s,
		"ping\r\nset counter 1\r\n1\r\nincr counter 41\r\nget counter\r\ndelete counter\r\nget counter\r\n")
	want := "PONG\r\nSTORED\r\nVALUE 42\r\nVALUE counter 2\r\n42\r\nEND\r\nDELETED\r\nEND"
	if got != want {
		t.Fatalf("unexpected responses:\nwant:\n%s\n\ngot:\n%s", want, got)
	}
}

func TestSessionReturnsDeterministicLimitErrors(t *testing.T) {
	kv := store.New(store.Config{
		MaxValueBytes:     4,
		MaxMemoryBytes:    8,
		ItemOverheadBytes: 0,
		Shards:            1,
	})
	s := New(kv)
	got := serveAll(t, s, "set big 5\r\n12345\r\nset full 4\r\n1234\r\nset too-wide 1\r\nx\r\n")
	want := "SERVER_ERROR value too large\r\nSTORED\r\nSERVER_ERROR memory limit exceeded"
	if got != want {
		t.Fatalf("unexpected responses:\nwant:\n%s\n\ngot:\n%s", want, got)
	}
}

func TestSessionEvictsOlderKeyUnderMemoryPressure(t *testing.T) {
	kv := store.New(store.Config{
		MaxValueBytes:     8,
		MaxMemoryBytes:    6,
		ItemOverheadBytes: 0,
		Shards:            1,
	})
	s := New(kv)
	got := serveAll(t, s, "set a 2\r\n11\r\nset b 2\r\n22\r\nget a\r\nset c 2\r\n33\r\nget b\r\nget a\r\nget c\r\n")
	want := "STORED\r\nSTORED\r\nVALUE a 2\r\n11\r\nEND\r\nSTORED\r\nEND\r\nVALUE a 2\r\n11\r\nEND\r\nVALUE c 2\r\n33\r\nEND"
	if got != want {
		t.Fatalf("unexpected responses:\nwant:\n%s\n\ngot:\n%s", want, got)
	}
}

func TestAuthGateClosesWhenFirstCommandIsNotAuth(t *testing.T) {
	s := New(store.New(store.DefaultConfig())).WithAuthToken("secret")
	got := serveAll(t, s, "ping\r\nset x 1\r\nv\r\n")
	want := "CLIENT_ERROR auth required"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestAuthGateRejectsWrongToken(t *testing.T) {
	s := New(store.New(store.DefaultConfig())).WithAuthToken("secret")
	got := serveAll(t, s, "auth wrong\r\nping\r\n")
	want := "CLIENT_ERROR auth failed"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestAuthGateAcceptsRightTokenAndUnlocksConnection(t *testing.T) {
	s := New(store.New(store.DefaultConfig())).WithAuthToken("secret")
	got := serveAll(t, s, "auth secret\r\nping\r\nset x 1\r\nv\r\nget x\r\n")
	want := "AUTHENTICATED\r\nPONG\r\nSTORED\r\nVALUE x 1\r\nv\r\nEND"
	if got != want {
		t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
	}
}

func TestAuthCommandRejectedWhenAuthNotConfigured(t *testing.T) {
	s := New(store.New(store.DefaultConfig()))
	got := serveAll(t, s, "auth anything\r\nping\r\n")
	want := "CLIENT_ERROR auth not configured"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMgetReturnsHitsAndSkipsMisses(t *testing.T) {
	s := New(store.New(store.DefaultConfig()))
	got := serveAll(t, s,
		"set a 1\r\nA\r\nset c 1\r\nC\r\nmget a b c\r\n")
	want := "STORED\r\nSTORED\r\nVALUE a 1\r\nA\r\nVALUE c 1\r\nC\r\nEND"
	if got != want {
		t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
	}
}

func TestGetsIncludesCASToken(t *testing.T) {
	s := New(store.New(store.DefaultConfig()))
	got := serveAll(t, s,
		"set k 1\r\nv\r\ngets k\r\n")
	// CAS is token-1 since set is the first mutation in the store.
	want := "STORED\r\nVALUE k 1 1\r\nv\r\nEND"
	if got != want {
		t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
	}
}

func TestCasRequiresMatchingVersion(t *testing.T) {
	s := New(store.New(store.DefaultConfig()))
	got := serveAll(t, s,
		"set k 1\r\nv\r\ncas k 999 1\r\nx\r\ngets k\r\ncas k 1 1\r\ny\r\ngets k\r\n")
	// First cas tries version 999 → EXISTS (mismatch); gets returns version 1.
	// Second cas uses version 1 (correct) → STORED, bumps to 2.
	want := "STORED\r\nEXISTS\r\nVALUE k 1 1\r\nv\r\nEND\r\nSTORED\r\nVALUE k 1 2\r\ny\r\nEND"
	if got != want {
		t.Fatalf("got:\n%s\n\nwant:\n%s", got, want)
	}
}

func TestCasOnMissingKeyReturnsNotFound(t *testing.T) {
	s := New(store.New(store.DefaultConfig()))
	got := serveAll(t, s, "cas k 1 1\r\nv\r\n")
	want := "NOT_FOUND"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
