package server

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Emrebener/Mini-Key-Value-Store/internal/store"
)

func TestSessionExecutesProtocolCommands(t *testing.T) {
	var output bytes.Buffer
	input := strings.NewReader("set counter 1\r\n1\r\nincr counter 41\r\nget counter\r\ndelete counter\r\nget counter\r\n")

	if err := Serve(input, &output, store.New(store.DefaultConfig())); err != nil {
		t.Fatalf("serve connection: %v", err)
	}

	got := strings.TrimSuffix(output.String(), "\r\n")
	want := "STORED\r\nVALUE 42\r\nVALUE counter 2\r\n42\r\nEND\r\nDELETED\r\nEND"
	if got != want {
		t.Fatalf("unexpected responses:\nwant:\n%s\n\ngot:\n%s", want, got)
	}
}

func TestSessionReturnsDeterministicLimitErrors(t *testing.T) {
	var output bytes.Buffer
	input := strings.NewReader("set big 5\r\n12345\r\nset full 4\r\n1234\r\nset too-wide 1\r\nx\r\n")
	kv := store.New(store.Config{
		MaxValueBytes:     4,
		MaxMemoryBytes:    8,
		ItemOverheadBytes: 0,
	})

	if err := Serve(input, &output, kv); err != nil {
		t.Fatalf("serve connection: %v", err)
	}

	got := strings.TrimSuffix(output.String(), "\r\n")
	want := "SERVER_ERROR value too large\r\nSTORED\r\nSERVER_ERROR memory limit exceeded"
	if got != want {
		t.Fatalf("unexpected responses:\nwant:\n%s\n\ngot:\n%s", want, got)
	}
}

func TestSessionEvictsOlderKeyUnderMemoryPressure(t *testing.T) {
	var output bytes.Buffer
	input := strings.NewReader("set a 2\r\n11\r\nset b 2\r\n22\r\nget a\r\nset c 2\r\n33\r\nget b\r\nget a\r\nget c\r\n")
	kv := store.New(store.Config{
		MaxValueBytes:     8,
		MaxMemoryBytes:    6,
		ItemOverheadBytes: 0,
	})

	if err := Serve(input, &output, kv); err != nil {
		t.Fatalf("serve connection: %v", err)
	}

	got := strings.TrimSuffix(output.String(), "\r\n")
	want := "STORED\r\nSTORED\r\nVALUE a 2\r\n11\r\nEND\r\nSTORED\r\nEND\r\nVALUE a 2\r\n11\r\nEND\r\nVALUE c 2\r\n33\r\nEND"
	if got != want {
		t.Fatalf("unexpected responses:\nwant:\n%s\n\ngot:\n%s", want, got)
	}
}
