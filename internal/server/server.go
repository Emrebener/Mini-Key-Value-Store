// Package server wires the wire-protocol parser and the in-memory store to a
// single TCP connection's lifecycle. One Server instance handles many
// connections concurrently; per-connection state (authentication, read
// deadlines) lives inside the goroutine that owns the connection.
package server

import (
	"bufio"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"

	"github.com/Emrebener/Mini-Key-Value-Store/internal/protocol"
	"github.com/Emrebener/Mini-Key-Value-Store/internal/store"
)

// Server holds the dependencies and configuration shared across all client
// connections handled by this process.
type Server struct {
	store     *store.Store
	authToken string
}

// New constructs a Server bound to kv. Call With* setters before serving to
// configure connection-level behavior (auth, etc.).
func New(kv *store.Store) *Server {
	return &Server{store: kv}
}

// WithAuthToken enables AUTH-gated connections. When token is empty (the
// zero value), no authentication is required and the AUTH command is
// rejected.
func (s *Server) WithAuthToken(token string) *Server {
	s.authToken = token
	return s
}

// ServeConn owns the lifecycle of a single TCP connection: it reads
// commands until EOF, an unrecoverable error, or an authentication
// failure, and then closes the connection.
func (s *Server) ServeConn(conn net.Conn) error {
	defer conn.Close()
	return s.Serve(conn, conn)
}

// Serve reads commands from input, executes them against the store, and
// writes responses to output. It returns nil on graceful EOF and an error
// for I/O or protocol failures the caller should log. Authentication
// failures are written to output as protocol responses, then Serve
// returns nil so the connection can close cleanly.
func (s *Server) Serve(input io.Reader, output io.Writer) error {
	parser := protocol.NewParser(bufio.NewReader(input))
	writer := bufio.NewWriter(output)
	defer writer.Flush()

	authenticated := s.authToken == ""

	for {
		command, err := parser.ReadCommand()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			writeLine(writer, "CLIENT_ERROR bad command")
			return writer.Flush()
		}

		if !authenticated {
			ok, terminal := s.handleAuthGate(writer, command)
			if !ok {
				if err := writer.Flush(); err != nil {
					return err
				}
				if terminal {
					return nil
				}
				continue
			}
			authenticated = true
			if err := writer.Flush(); err != nil {
				return err
			}
			continue
		}

		if command.Op == protocol.OpAuth {
			if s.authToken == "" {
				writeLine(writer, "CLIENT_ERROR auth not configured")
				return writer.Flush()
			}
			// Already authenticated; idempotent re-AUTH succeeds if the
			// token still matches.
			if !constantTimeEqual(command.Token, s.authToken) {
				writeLine(writer, "CLIENT_ERROR auth failed")
				return writer.Flush()
			}
			writeLine(writer, "AUTHENTICATED")
			if err := writer.Flush(); err != nil {
				return err
			}
			continue
		}

		if err := execute(writer, s.store, command); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
}

// handleAuthGate processes the first command on an auth-required connection.
// It returns (ok, terminal): ok is true when the command was a successful
// AUTH and the connection should continue authenticated; terminal is true
// when the connection must close (failed AUTH or non-AUTH first command).
func (s *Server) handleAuthGate(writer *bufio.Writer, command protocol.Command) (ok, terminal bool) {
	if command.Op != protocol.OpAuth {
		writeLine(writer, "CLIENT_ERROR auth required")
		return false, true
	}
	if !constantTimeEqual(command.Token, s.authToken) {
		writeLine(writer, "CLIENT_ERROR auth failed")
		return false, true
	}
	writeLine(writer, "AUTHENTICATED")
	return true, false
}

func constantTimeEqual(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func execute(writer *bufio.Writer, kv *store.Store, command protocol.Command) error {
	switch command.Op {
	case protocol.OpPing:
		return writeLine(writer, "PONG")
	case protocol.OpGet:
		item, ok := kv.Get(command.Key)
		if !ok {
			return writeLine(writer, "END")
		}
		if err := writeLine(writer, fmt.Sprintf("VALUE %s %d", command.Key, len(item.Value))); err != nil {
			return err
		}
		if _, err := writer.Write(item.Value); err != nil {
			return err
		}
		if _, err := writer.WriteString("\r\n"); err != nil {
			return err
		}
		return writeLine(writer, "END")
	case protocol.OpSet:
		if command.TTLSeconds > uint64(math.MaxInt64/int64(time.Second)) {
			return writeLine(writer, "CLIENT_ERROR TTL is too large")
		}
		ttl := time.Duration(command.TTLSeconds) * time.Second
		err := kv.Set(command.Key, command.Value, ttl)
		switch {
		case err == nil:
			return writeLine(writer, "STORED")
		case errors.Is(err, store.ErrValueTooLarge):
			return writeLine(writer, "SERVER_ERROR value too large")
		case errors.Is(err, store.ErrMemoryLimitExceeded):
			return writeLine(writer, "SERVER_ERROR memory limit exceeded")
		default:
			return err
		}
	case protocol.OpDelete:
		if kv.Delete(command.Key) {
			return writeLine(writer, "DELETED")
		}
		return writeLine(writer, "NOT_FOUND")
	case protocol.OpMget:
		for _, key := range command.Keys {
			item, ok := kv.Get(key)
			if !ok {
				continue
			}
			if err := writeLine(writer, fmt.Sprintf("VALUE %s %d", key, len(item.Value))); err != nil {
				return err
			}
			if _, err := writer.Write(item.Value); err != nil {
				return err
			}
			if _, err := writer.WriteString("\r\n"); err != nil {
				return err
			}
		}
		return writeLine(writer, "END")
	case protocol.OpGets:
		for _, key := range command.Keys {
			item, ok := kv.Get(key)
			if !ok {
				continue
			}
			if err := writeLine(writer, fmt.Sprintf("VALUE %s %d %d", key, len(item.Value), item.CAS)); err != nil {
				return err
			}
			if _, err := writer.Write(item.Value); err != nil {
				return err
			}
			if _, err := writer.WriteString("\r\n"); err != nil {
				return err
			}
		}
		return writeLine(writer, "END")
	case protocol.OpCas:
		if command.TTLSeconds > uint64(math.MaxInt64/int64(time.Second)) {
			return writeLine(writer, "CLIENT_ERROR TTL is too large")
		}
		ttl := time.Duration(command.TTLSeconds) * time.Second
		err := kv.Cas(command.Key, command.Value, ttl, command.CAS)
		switch {
		case err == nil:
			return writeLine(writer, "STORED")
		case errors.Is(err, store.ErrCasMismatch):
			return writeLine(writer, "EXISTS")
		case errors.Is(err, store.ErrNotFound):
			return writeLine(writer, "NOT_FOUND")
		case errors.Is(err, store.ErrValueTooLarge):
			return writeLine(writer, "SERVER_ERROR value too large")
		case errors.Is(err, store.ErrMemoryLimitExceeded):
			return writeLine(writer, "SERVER_ERROR memory limit exceeded")
		default:
			return err
		}
	case protocol.OpIncr:
		value, err := kv.Incr(command.Key, command.Delta)
		switch {
		case err == nil:
			return writeLine(writer, fmt.Sprintf("VALUE %d", value))
		case errors.Is(err, store.ErrNotFound):
			return writeLine(writer, "NOT_FOUND")
		case errors.Is(err, store.ErrNotInteger):
			return writeLine(writer, "CLIENT_ERROR value is not an unsigned integer")
		case errors.Is(err, store.ErrOverflow):
			return writeLine(writer, "CLIENT_ERROR increment would overflow uint64")
		case errors.Is(err, store.ErrValueTooLarge):
			return writeLine(writer, "SERVER_ERROR value too large")
		case errors.Is(err, store.ErrMemoryLimitExceeded):
			return writeLine(writer, "SERVER_ERROR memory limit exceeded")
		default:
			return err
		}
	default:
		return writeLine(writer, "CLIENT_ERROR unknown command")
	}
}

func writeLine(writer *bufio.Writer, line string) error {
	if _, err := writer.WriteString(line); err != nil {
		return err
	}
	_, err := writer.WriteString("\r\n")
	return err
}
