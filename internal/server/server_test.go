package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

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

// startTestServer brings the server up on a real localhost listener so
// tests can exercise the connection-level lifecycle (deadlines, semaphore,
// shutdown). Returns the dial address and a stop function.
func startTestServer(t *testing.T, srv *Server) (addr string, stop func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()
	stop = func() {
		_ = listener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		wg.Wait()
	}
	return listener.Addr().String(), stop
}

func TestMaxConnectionsRejectsBeyondCap(t *testing.T) {
	srv := New(store.New(store.DefaultConfig())).WithMaxConnections(1)
	addr, stop := startTestServer(t, srv)
	defer stop()

	// First connection holds its slot by sending a command and reading the response.
	first, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	defer first.Close()
	if _, err := io.WriteString(first, "ping\r\n"); err != nil {
		t.Fatalf("write first: %v", err)
	}
	firstResp, err := bufio.NewReader(first).ReadString('\n')
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	if !strings.HasPrefix(firstResp, "PONG") {
		t.Fatalf("first response = %q, want PONG", firstResp)
	}

	// Second connection should be rejected with SERVER_ERROR and closed.
	second, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial second: %v", err)
	}
	defer second.Close()
	body, err := io.ReadAll(second)
	if err != nil {
		t.Fatalf("read second: %v", err)
	}
	if !strings.Contains(string(body), "SERVER_ERROR max connections reached") {
		t.Errorf("rejection body = %q, want SERVER_ERROR max connections reached", body)
	}
}

func TestIdleTimeoutClosesQuietConnection(t *testing.T) {
	srv := New(store.New(store.DefaultConfig())).WithIdleTimeout(50 * time.Millisecond)
	addr, stop := startTestServer(t, srv)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Don't send anything. The server should close the connection after the idle timeout.
	deadline := time.Now().Add(time.Second)
	_ = conn.SetReadDeadline(deadline)
	_, err = io.ReadAll(conn)
	if err != nil && !errors.Is(err, io.EOF) && !strings.Contains(err.Error(), "use of closed") {
		// io.EOF or a generic close is acceptable; what we don't want is a deadline-on-the-client error.
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Fatalf("client read timed out before server closed; idle timeout not enforced")
		}
	}
}

func TestShutdownCompletesAfterDrainingActiveConnection(t *testing.T) {
	srv := New(store.New(store.DefaultConfig()))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var acceptWG sync.WaitGroup
	acceptWG.Add(1)
	go func() {
		defer acceptWG.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "ping\r\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.HasPrefix(resp, "PONG") {
		t.Fatalf("response = %q", resp)
	}

	// Connection is now idle on the server. Shutdown should evict it
	// promptly via the read-deadline mechanism.
	_ = listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Shutdown took %v, want < 1s", elapsed)
	}
	acceptWG.Wait()
}

func TestStatsReportsCommandCountersAndStoreItems(t *testing.T) {
	s := New(store.New(store.DefaultConfig()))
	out := serveAll(t, s, "set a 1\r\nA\r\nset b 1\r\nB\r\nget a\r\nstats\r\n")

	if !strings.Contains(out, "STAT cmd_set 2") {
		t.Errorf("expected STAT cmd_set 2 in output, got:\n%s", out)
	}
	if !strings.Contains(out, "STAT cmd_get 1") {
		t.Errorf("expected STAT cmd_get 1 in output, got:\n%s", out)
	}
	if !strings.Contains(out, "STAT cmd_stats 1") {
		t.Errorf("expected STAT cmd_stats 1 in output, got:\n%s", out)
	}
	if !strings.Contains(out, "STAT items 2") {
		t.Errorf("expected STAT items 2 in output, got:\n%s", out)
	}
}
