package protocol

import (
	"bufio"
	"errors"
	"strings"
	"testing"
)

func TestParserReadsSetWithByteCountedValue(t *testing.T) {
	p := NewParser(bufio.NewReader(strings.NewReader("set greeting 11\r\nhello world\r\n")))

	cmd, err := p.ReadCommand()
	if err != nil {
		t.Fatalf("expected command to parse: %v", err)
	}
	if cmd.Op != OpSet {
		t.Fatalf("expected OpSet, got %v", cmd.Op)
	}
	if cmd.Key != "greeting" {
		t.Fatalf("expected key greeting, got %q", cmd.Key)
	}
	if string(cmd.Value) != "hello world" {
		t.Fatalf("expected byte-counted value, got %q", cmd.Value)
	}
}

func TestParserReadsSingleLineCommands(t *testing.T) {
	p := NewParser(bufio.NewReader(strings.NewReader("get alpha\r\ndelete beta\r\nincr visits 3\r\n")))

	first, err := p.ReadCommand()
	if err != nil {
		t.Fatalf("expected get command: %v", err)
	}
	if first.Op != OpGet || first.Key != "alpha" {
		t.Fatalf("unexpected get command: %#v", first)
	}

	second, err := p.ReadCommand()
	if err != nil {
		t.Fatalf("expected delete command: %v", err)
	}
	if second.Op != OpDelete || second.Key != "beta" {
		t.Fatalf("unexpected delete command: %#v", second)
	}

	third, err := p.ReadCommand()
	if err != nil {
		t.Fatalf("expected incr command: %v", err)
	}
	if third.Op != OpIncr || third.Key != "visits" || third.Delta != 3 {
		t.Fatalf("unexpected incr command: %#v", third)
	}
}

func TestParserRejectsMalformedCommandsDeterministically(t *testing.T) {
	tests := []string{
		"set key nope\r\n",
		"set key 4\r\nabc\r\n",
		"get has space\r\n",
		"incr key -1\r\n",
		"unknown key\r\n",
	}

	for _, input := range tests {
		t.Run(strings.TrimSpace(input), func(t *testing.T) {
			p := NewParser(bufio.NewReader(strings.NewReader(input)))
			_, err := p.ReadCommand()
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("expected ErrProtocol, got %v", err)
			}
		})
	}
}
