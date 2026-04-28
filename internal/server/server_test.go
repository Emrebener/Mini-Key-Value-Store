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

	if err := Serve(input, &output, store.New()); err != nil {
		t.Fatalf("serve connection: %v", err)
	}

	got := strings.TrimSuffix(output.String(), "\r\n")
	want := "STORED\r\nVALUE 42\r\nVALUE counter 2\r\n42\r\nEND\r\nDELETED\r\nEND"
	if got != want {
		t.Fatalf("unexpected responses:\nwant:\n%s\n\ngot:\n%s", want, got)
	}
}
