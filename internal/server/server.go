// Package server wires the wire-protocol parser and the in-memory store to a
// single TCP connection's lifecycle. One Server instance handles many
// connections concurrently; per-connection state (authentication, read
// deadlines) lives inside the goroutine that owns the connection.
package server

import (
	"bufio"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Emrebener/Mini-Key-Value-Store/internal/protocol"
	"github.com/Emrebener/Mini-Key-Value-Store/internal/store"
)

// Server holds the dependencies and configuration shared across all client
// connections handled by this process.
type Server struct {
	store       *store.Store
	authToken   string
	idleTimeout time.Duration
	semaphore   chan struct{}
	startedAt   time.Time

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup

	// Operator-facing counters. Bumped from connection goroutines via
	// atomic ops; sampled by Stats() for STATS / /doctor responses.
	connectionsOpened   atomic.Uint64
	connectionsActive   atomic.Int64
	connectionsRejected atomic.Uint64
	authFailures        atomic.Uint64
	clientErrors        atomic.Uint64
	serverErrors        atomic.Uint64

	cmdGet    atomic.Uint64
	cmdSet    atomic.Uint64
	cmdDelete atomic.Uint64
	cmdIncr   atomic.Uint64
	cmdMget   atomic.Uint64
	cmdGets   atomic.Uint64
	cmdCas    atomic.Uint64
	cmdStats  atomic.Uint64
	cmdAuth   atomic.Uint64
	cmdPing   atomic.Uint64
}

// New constructs a Server bound to kv. Call With* setters before serving to
// configure connection-level behavior (auth, deadlines, max connections).
func New(kv *store.Store) *Server {
	return &Server{
		store:     kv,
		startedAt: time.Now(),
		conns:     make(map[net.Conn]struct{}),
	}
}

// WithAuthToken enables AUTH-gated connections. When token is empty (the
// zero value), no authentication is required and the AUTH command is
// rejected.
func (s *Server) WithAuthToken(token string) *Server {
	s.authToken = token
	return s
}

// WithIdleTimeout sets the per-connection read deadline applied before
// each command. Zero or negative disables the timeout.
func (s *Server) WithIdleTimeout(d time.Duration) *Server {
	s.idleTimeout = d
	return s
}

// WithMaxConnections caps the number of in-flight connections. Past the
// cap, new connections receive SERVER_ERROR max connections reached and
// are closed. Zero or negative disables the cap.
func (s *Server) WithMaxConnections(n int) *Server {
	if n > 0 {
		s.semaphore = make(chan struct{}, n)
	} else {
		s.semaphore = nil
	}
	return s
}

// Store returns the backing store. Exposed so callers (e.g. the operations
// HTTP listener) can sample it without a separate handle.
func (s *Server) Store() *store.Store { return s.store }

// Stats is a snapshot of operator-facing counters and store stats.
type Stats struct {
	StartedAt           time.Time
	Uptime              time.Duration
	ConnectionsOpened   uint64
	ConnectionsActive   int64
	ConnectionsRejected uint64
	AuthFailures        uint64
	ClientErrors        uint64
	ServerErrors        uint64
	CmdGet              uint64
	CmdSet              uint64
	CmdDelete           uint64
	CmdIncr             uint64
	CmdMget             uint64
	CmdGets             uint64
	CmdCas              uint64
	CmdStats            uint64
	CmdAuth             uint64
	CmdPing             uint64
	Store               store.Stats
}

func (s *Server) Stats() Stats {
	now := time.Now()
	return Stats{
		StartedAt:           s.startedAt,
		Uptime:              now.Sub(s.startedAt),
		ConnectionsOpened:   s.connectionsOpened.Load(),
		ConnectionsActive:   s.connectionsActive.Load(),
		ConnectionsRejected: s.connectionsRejected.Load(),
		AuthFailures:        s.authFailures.Load(),
		ClientErrors:        s.clientErrors.Load(),
		ServerErrors:        s.serverErrors.Load(),
		CmdGet:              s.cmdGet.Load(),
		CmdSet:              s.cmdSet.Load(),
		CmdDelete:           s.cmdDelete.Load(),
		CmdIncr:             s.cmdIncr.Load(),
		CmdMget:             s.cmdMget.Load(),
		CmdGets:             s.cmdGets.Load(),
		CmdCas:              s.cmdCas.Load(),
		CmdStats:            s.cmdStats.Load(),
		CmdAuth:             s.cmdAuth.Load(),
		CmdPing:             s.cmdPing.Load(),
		Store:               s.store.Stats(),
	}
}

// ServeConn owns the lifecycle of a single TCP connection: it tries to
// reserve a slot in the connection cap, sets read deadlines per command,
// reads commands until EOF, an unrecoverable error, or shutdown, and
// then closes the connection.
func (s *Server) ServeConn(conn net.Conn) error {
	s.connectionsOpened.Add(1)

	if !s.acquireSlot() {
		s.connectionsRejected.Add(1)
		_, _ = conn.Write([]byte("SERVER_ERROR max connections reached\r\n"))
		_ = conn.Close()
		return nil
	}
	defer s.releaseSlot()

	s.wg.Add(1)
	defer s.wg.Done()
	s.registerConn(conn)
	defer s.deregisterConn(conn)

	s.connectionsActive.Add(1)
	defer s.connectionsActive.Add(-1)
	defer conn.Close()

	return s.serveConn(conn)
}

func (s *Server) acquireSlot() bool {
	if s.semaphore == nil {
		return true
	}
	select {
	case s.semaphore <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseSlot() {
	if s.semaphore == nil {
		return
	}
	<-s.semaphore
}

func (s *Server) registerConn(conn net.Conn) {
	s.connsMu.Lock()
	s.conns[conn] = struct{}{}
	s.connsMu.Unlock()
}

func (s *Server) deregisterConn(conn net.Conn) {
	s.connsMu.Lock()
	delete(s.conns, conn)
	s.connsMu.Unlock()
}

// Shutdown asks every active connection to wind down by setting an
// immediate read deadline. Connection handlers exit naturally as their
// in-flight reads fail, draining via the internal WaitGroup. If ctx
// expires before the wait completes, surviving connections are
// force-closed and ctx.Err() is returned.
func (s *Server) Shutdown(ctx context.Context) error {
	s.connsMu.Lock()
	for c := range s.conns {
		_ = c.SetReadDeadline(time.Now())
	}
	s.connsMu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.connsMu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.connsMu.Unlock()
		<-done
		return ctx.Err()
	}
}

// serveConn is the connection-aware loop. It applies idle deadlines and
// dispatches commands. Tests against Serve(input, output) bypass it.
func (s *Server) serveConn(conn net.Conn) error {
	parser := protocol.NewParser(bufio.NewReader(conn))
	writer := bufio.NewWriter(conn)
	defer writer.Flush()

	authenticated := s.authToken == ""

	for {
		if s.idleTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.idleTimeout))
		}
		command, err := parser.ReadCommand()
		if err != nil {
			return s.handleReadError(writer, err, authenticated)
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
			s.cmdAuth.Add(1)
			if s.authToken == "" {
				writeLine(writer, "CLIENT_ERROR auth not configured")
				return writer.Flush()
			}
			if !constantTimeEqual(command.Token, s.authToken) {
				s.authFailures.Add(1)
				writeLine(writer, "CLIENT_ERROR auth failed")
				return writer.Flush()
			}
			writeLine(writer, "AUTHENTICATED")
			if err := writer.Flush(); err != nil {
				return err
			}
			continue
		}

		s.countCommand(command.Op)

		if err := s.execute(writer, command); err != nil {
			s.serverErrors.Add(1)
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
}

// Serve reads commands from input, executes them against the store, and
// writes responses to output. Used by tests; production traffic goes
// through ServeConn instead.
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
			s.clientErrors.Add(1)
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
			s.cmdAuth.Add(1)
			if s.authToken == "" {
				writeLine(writer, "CLIENT_ERROR auth not configured")
				return writer.Flush()
			}
			if !constantTimeEqual(command.Token, s.authToken) {
				s.authFailures.Add(1)
				writeLine(writer, "CLIENT_ERROR auth failed")
				return writer.Flush()
			}
			writeLine(writer, "AUTHENTICATED")
			if err := writer.Flush(); err != nil {
				return err
			}
			continue
		}

		s.countCommand(command.Op)

		if err := s.execute(writer, command); err != nil {
			s.serverErrors.Add(1)
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
}

// handleReadError translates a parser error into the right protocol
// response and a return value for the connection loop.
func (s *Server) handleReadError(writer *bufio.Writer, err error, _ bool) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		// Idle timeout or shutdown deadline. Close quietly.
		return nil
	}
	s.clientErrors.Add(1)
	writeLine(writer, "CLIENT_ERROR bad command")
	return writer.Flush()
}

func (s *Server) countCommand(op protocol.Op) {
	switch op {
	case protocol.OpGet:
		s.cmdGet.Add(1)
	case protocol.OpSet:
		s.cmdSet.Add(1)
	case protocol.OpDelete:
		s.cmdDelete.Add(1)
	case protocol.OpIncr:
		s.cmdIncr.Add(1)
	case protocol.OpMget:
		s.cmdMget.Add(1)
	case protocol.OpGets:
		s.cmdGets.Add(1)
	case protocol.OpCas:
		s.cmdCas.Add(1)
	case protocol.OpStats:
		s.cmdStats.Add(1)
	case protocol.OpPing:
		s.cmdPing.Add(1)
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
	s.cmdAuth.Add(1)
	if !constantTimeEqual(command.Token, s.authToken) {
		s.authFailures.Add(1)
		writeLine(writer, "CLIENT_ERROR auth failed")
		return false, true
	}
	writeLine(writer, "AUTHENTICATED")
	return true, false
}

func constantTimeEqual(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *Server) execute(writer *bufio.Writer, command protocol.Command) error {
	kv := s.store
	switch command.Op {
	case protocol.OpPing:
		return writeLine(writer, "PONG")
	case protocol.OpStats:
		return writeStats(writer, s.Stats())
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

func writeStats(writer *bufio.Writer, st Stats) error {
	lines := []struct {
		name  string
		value string
	}{
		{"uptime_seconds", fmt.Sprintf("%d", int64(st.Uptime.Seconds()))},
		{"connections_opened", fmt.Sprintf("%d", st.ConnectionsOpened)},
		{"connections_active", fmt.Sprintf("%d", st.ConnectionsActive)},
		{"connections_rejected", fmt.Sprintf("%d", st.ConnectionsRejected)},
		{"auth_failures", fmt.Sprintf("%d", st.AuthFailures)},
		{"client_errors", fmt.Sprintf("%d", st.ClientErrors)},
		{"server_errors", fmt.Sprintf("%d", st.ServerErrors)},
		{"cmd_get", fmt.Sprintf("%d", st.CmdGet)},
		{"cmd_set", fmt.Sprintf("%d", st.CmdSet)},
		{"cmd_delete", fmt.Sprintf("%d", st.CmdDelete)},
		{"cmd_incr", fmt.Sprintf("%d", st.CmdIncr)},
		{"cmd_mget", fmt.Sprintf("%d", st.CmdMget)},
		{"cmd_gets", fmt.Sprintf("%d", st.CmdGets)},
		{"cmd_cas", fmt.Sprintf("%d", st.CmdCas)},
		{"cmd_stats", fmt.Sprintf("%d", st.CmdStats)},
		{"cmd_auth", fmt.Sprintf("%d", st.CmdAuth)},
		{"cmd_ping", fmt.Sprintf("%d", st.CmdPing)},
		{"items", fmt.Sprintf("%d", st.Store.Items)},
		{"memory_bytes", fmt.Sprintf("%d", st.Store.MemoryBytes)},
		{"max_memory_bytes", fmt.Sprintf("%d", st.Store.MaxMemoryBytes)},
		{"evictions", fmt.Sprintf("%d", st.Store.Evictions)},
		{"expirations", fmt.Sprintf("%d", st.Store.Expirations)},
		{"shards", fmt.Sprintf("%d", st.Store.NumShards)},
	}
	for _, l := range lines {
		if err := writeLine(writer, "STAT "+l.name+" "+l.value); err != nil {
			return err
		}
	}
	return writeLine(writer, "END")
}

func writeLine(writer *bufio.Writer, line string) error {
	if _, err := writer.WriteString(line); err != nil {
		return err
	}
	_, err := writer.WriteString("\r\n")
	return err
}
