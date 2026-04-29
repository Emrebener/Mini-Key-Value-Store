package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const MaxKeyBytes = 250

var ErrProtocol = errors.New("protocol error")

type Op int

const (
	OpGet Op = iota + 1
	OpSet
	OpDelete
	OpIncr
	OpPing
)

type Command struct {
	Op         Op
	Key        string
	Value      []byte
	TTLSeconds uint64
	Delta      uint64
}

type Parser struct {
	reader *bufio.Reader
}

func NewParser(reader *bufio.Reader) *Parser {
	return &Parser{reader: reader}
}

func (p *Parser) ReadCommand() (Command, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Command{}, io.EOF
		}
		return Command{}, err
	}
	line, err = trimCRLF(line)
	if err != nil {
		return Command{}, err
	}

	fields := strings.Fields(line)
	if len(fields) == 0 {
		return Command{}, protocolError("empty command")
	}

	switch strings.ToLower(fields[0]) {
	case "ping":
		if len(fields) != 1 {
			return Command{}, protocolError("usage: ping")
		}
		return Command{Op: OpPing}, nil
	case "get":
		if len(fields) != 2 || !validKey(fields[1]) {
			return Command{}, protocolError("usage: get <key>")
		}
		return Command{Op: OpGet, Key: fields[1]}, nil
	case "set":
		if (len(fields) != 3 && len(fields) != 4) || !validKey(fields[1]) {
			return Command{}, protocolError("usage: set <key> [ttl-seconds] <bytes>")
		}
		ttlSeconds := uint64(0)
		sizeToken := fields[2]
		if len(fields) == 4 {
			ttl, err := parseUintToken(fields[2], "TTL")
			if err != nil {
				return Command{}, err
			}
			ttlSeconds = ttl
			sizeToken = fields[3]
		}
		size, err := parseByteCount(sizeToken)
		if err != nil {
			return Command{}, err
		}
		value := make([]byte, size+2)
		if _, err := io.ReadFull(p.reader, value); err != nil {
			return Command{}, protocolError("incomplete value")
		}
		if value[size] != '\r' || value[size+1] != '\n' {
			return Command{}, protocolError("value must end with CRLF")
		}
		return Command{Op: OpSet, Key: fields[1], Value: value[:size], TTLSeconds: ttlSeconds}, nil
	case "delete":
		if len(fields) != 2 || !validKey(fields[1]) {
			return Command{}, protocolError("usage: delete <key>")
		}
		return Command{Op: OpDelete, Key: fields[1]}, nil
	case "incr":
		if len(fields) != 3 || !validKey(fields[1]) {
			return Command{}, protocolError("usage: incr <key> <delta>")
		}
		delta, err := parseUintToken(fields[2], "delta")
		if err != nil {
			return Command{}, err
		}
		return Command{Op: OpIncr, Key: fields[1], Delta: delta}, nil
	default:
		return Command{}, protocolError("unknown command")
	}
}

func trimCRLF(line string) (string, error) {
	if !strings.HasSuffix(line, "\r\n") {
		return "", protocolError("command line must end with CRLF")
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func parseByteCount(token string) (int, error) {
	value, err := parseUintToken(token, "byte count")
	if err != nil {
		return 0, err
	}
	if value > uint64(^uint(0)>>1) {
		return 0, protocolError("byte count is too large")
	}
	return int(value), nil
}

func parseUintToken(token string, name string) (uint64, error) {
	if token == "" {
		return 0, protocolError(name + " must be an unsigned integer")
	}
	for _, r := range token {
		if r < '0' || r > '9' {
			return 0, protocolError(name + " must be an unsigned integer")
		}
	}
	value, err := strconv.ParseUint(token, 10, 64)
	if err != nil {
		return 0, protocolError(name + " is too large")
	}
	return value, nil
}

func validKey(key string) bool {
	if key == "" || len(key) > MaxKeyBytes {
		return false
	}
	for _, r := range key {
		if r <= ' ' || r == 0x7f {
			return false
		}
	}
	return true
}

func protocolError(message string) error {
	return fmt.Errorf("%w: %s", ErrProtocol, message)
}
