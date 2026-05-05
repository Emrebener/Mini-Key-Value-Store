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
	OpAuth
	OpMget
	OpGets
	OpCas
	OpStats
)

type Command struct {
	Op         Op
	Key        string
	Keys       []string // populated for OpMget and OpGets
	Value      []byte
	TTLSeconds uint64
	Delta      uint64
	Token      string
	CAS        uint64 // populated for OpCas
}

type Parser struct {
	reader *bufio.Reader
}

func NewParser(reader *bufio.Reader) *Parser {
	return &Parser{reader: reader}
}

func (p *Parser) ReadCommand() (Command, error) {
	lineBytes, err := readLineSlice(p.reader)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Command{}, io.EOF
		}
		return Command{}, err
	}
	if len(lineBytes) < 2 || lineBytes[len(lineBytes)-2] != '\r' {
		return Command{}, protocolError("command line must end with CRLF")
	}
	line := string(lineBytes[:len(lineBytes)-2])

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
	case "auth":
		if len(fields) != 2 || !validToken(fields[1]) {
			return Command{}, protocolError("usage: auth <token>")
		}
		return Command{Op: OpAuth, Token: fields[1]}, nil
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
	case "stats":
		if len(fields) != 1 {
			return Command{}, protocolError("usage: stats")
		}
		return Command{Op: OpStats}, nil
	case "mget":
		keys, err := keysFromFields(fields[1:])
		if err != nil {
			return Command{}, err
		}
		return Command{Op: OpMget, Keys: keys}, nil
	case "gets":
		keys, err := keysFromFields(fields[1:])
		if err != nil {
			return Command{}, err
		}
		return Command{Op: OpGets, Keys: keys}, nil
	case "cas":
		// cas <key> [<ttl-seconds>] <cas-version> <bytes>\r\n<value>\r\n
		if len(fields) != 4 && len(fields) != 5 || !validKey(fields[1]) {
			return Command{}, protocolError("usage: cas <key> [<ttl-seconds>] <cas-version> <bytes>")
		}
		ttlSeconds := uint64(0)
		var versionToken, sizeToken string
		if len(fields) == 4 {
			versionToken = fields[2]
			sizeToken = fields[3]
		} else {
			ttl, err := parseUintToken(fields[2], "TTL")
			if err != nil {
				return Command{}, err
			}
			ttlSeconds = ttl
			versionToken = fields[3]
			sizeToken = fields[4]
		}
		version, err := parseUintToken(versionToken, "cas-version")
		if err != nil {
			return Command{}, err
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
		return Command{
			Op:         OpCas,
			Key:        fields[1],
			Value:      value[:size],
			TTLSeconds: ttlSeconds,
			CAS:        version,
		}, nil
	default:
		return Command{}, protocolError("unknown command")
	}
}

func keysFromFields(fields []string) ([]string, error) {
	if len(fields) == 0 {
		return nil, protocolError("at least one key is required")
	}
	for _, key := range fields {
		if !validKey(key) {
			return nil, protocolError("invalid key")
		}
	}
	return fields, nil
}

// readLineSlice reads through the next '\n' and returns a slice into the
// reader's internal buffer. The caller must consume the slice before the next
// read — that's why the parser stringifies it immediately.
func readLineSlice(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protocolError("command line too long")
	}
	return line, err
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

// validToken accepts the same character class as keys: any non-control,
// non-whitespace byte. Tokens are typically random strings produced by the
// operator's secret manager, so we don't restrict to alphanumeric.
func validToken(token string) bool {
	return validKey(token)
}

func protocolError(message string) error {
	return fmt.Errorf("%w: %s", ErrProtocol, message)
}
