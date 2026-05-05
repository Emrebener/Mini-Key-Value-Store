package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Emrebener/Mini-Key-Value-Store/internal/config"
	"github.com/Emrebener/Mini-Key-Value-Store/internal/server"
	"github.com/Emrebener/Mini-Key-Value-Store/internal/store"
)

const defaultConfigPath = "minikv.conf"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg, err := loadConfig()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	if err := run(context.Background(), cfg, logger); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func loadConfig() (config.Config, error) {
	path := flag.String("config", "", "path to config file (default: ./"+defaultConfigPath+")")
	flag.Parse()

	resolved := *path
	explicit := resolved != ""
	if !explicit {
		resolved = defaultConfigPath
	}

	cfg, err := config.Load(resolved)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !explicit {
			return config.Config{}, fmt.Errorf("config file %q not found in current directory; pass -config to specify another path", defaultConfigPath)
		}
		return config.Config{}, err
	}
	return cfg, nil
}

func run(parent context.Context, cfg config.Config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, scheme, err := listen(cfg)
	if err != nil {
		return err
	}
	defer listener.Close()

	logger.Info("listening", "addr", listener.Addr().String(), "scheme", scheme, "auth", cfg.AuthToken != "")

	kv := store.New(store.Config{
		MaxValueBytes:     cfg.MaxValueBytes,
		MaxMemoryBytes:    cfg.MaxMemoryBytes,
		ItemOverheadBytes: cfg.ItemOverheadBytes,
		Shards:            cfg.Shards,
	})
	srv := server.New(kv).WithAuthToken(cfg.AuthToken)
	var wg sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	go cleanupExpired(ctx, kv, cfg.CleanupInterval, logger)

	if cfg.PprofAddr != "" {
		go serveOps(ctx, cfg.PprofAddr, srv, logger)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, net.ErrClosed) {
				wg.Wait()
				return nil
			}
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.ServeConn(conn); err != nil {
				logger.Warn("connection closed with error", "remote", conn.RemoteAddr().String(), "error", err)
			}
		}()
	}
}

// listen returns a listener bound per cfg, plus a scheme tag ("tcp" or
// "tls") for the startup log.
func listen(cfg config.Config) (net.Listener, string, error) {
	if !cfg.TLSEnabled() {
		l, err := net.Listen("tcp", cfg.Addr)
		return l, "tcp", err
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, "", fmt.Errorf("load tls keypair: %w", err)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	l, err := tls.Listen("tcp", cfg.Addr, tlsCfg)
	return l, "tls", err
}

// serveOps runs the HTTP listener that exposes /healthz, /doctor, and the
// /debug/pprof tree. Routes are explicitly registered on a private mux so
// the pprof handlers don't leak onto http.DefaultServeMux.
func serveOps(ctx context.Context, addr string, srv *server.Server, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler)
	mux.HandleFunc("/doctor", doctorHandler(srv))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	httpSrv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdown)
	}()
	logger.Info("ops listening", "addr", addr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Warn("ops server stopped", "error", err)
	}
}

// healthzHandler answers liveness probes. The handler runs at all because
// the HTTP listener accepted the request, so the answer is unconditionally
// "ok"; the diagnostic equivalent lives at /doctor.
func healthzHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// doctorHandler returns 200 with a body listing every check, or 503 if any
// check tripped. The body is one "name: status reason" line per check, in
// stable order.
func doctorHandler(srv *server.Server) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		st := srv.Stats()
		failures := []string{}
		ok := []string{}

		// Memory pressure: warn when accounted bytes are above 95% of the
		// configured budget.
		var pressure float64
		if st.Store.MaxMemoryBytes > 0 {
			pressure = float64(st.Store.MemoryBytes) / float64(st.Store.MaxMemoryBytes)
		}
		if pressure > 0.95 {
			failures = append(failures, fmt.Sprintf("memory_pressure: high (%.0f%% of budget)", pressure*100))
		} else {
			ok = append(ok, fmt.Sprintf("memory_pressure: ok (%.0f%% of budget)", pressure*100))
		}

		// Shard balance: max-items / min-items ratio. Suppressed at very
		// small item counts because the ratio is dominated by hash noise.
		if st.Store.Items >= 100 {
			minItems, maxItems := st.Store.ItemsPerShard[0], st.Store.ItemsPerShard[0]
			for _, n := range st.Store.ItemsPerShard {
				if n < minItems {
					minItems = n
				}
				if n > maxItems {
					maxItems = n
				}
			}
			if minItems == 0 {
				minItems = 1
			}
			ratio := float64(maxItems) / float64(minItems)
			if ratio > 4 {
				failures = append(failures, fmt.Sprintf("shard_balance: imbalanced (max/min ratio %.1fx)", ratio))
			} else {
				ok = append(ok, fmt.Sprintf("shard_balance: ok (max/min ratio %.1fx)", ratio))
			}
		} else {
			ok = append(ok, fmt.Sprintf("shard_balance: ok (only %d items, ratio not meaningful)", st.Store.Items))
		}

		// Uptime is informational, never a failure.
		ok = append(ok, fmt.Sprintf("uptime: %s", st.Uptime.Round(time.Second)))

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if len(failures) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			for _, line := range failures {
				_, _ = w.Write([]byte("FAIL " + line + "\n"))
			}
		} else {
			w.WriteHeader(http.StatusOK)
		}
		for _, line := range ok {
			_, _ = w.Write([]byte("OK   " + line + "\n"))
		}
	}
}

func cleanupExpired(ctx context.Context, store *store.Store, interval time.Duration, logger *slog.Logger) {
	if interval == 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if removed := store.CleanupExpired(); removed > 0 {
				logger.Debug("cleaned expired keys", "count", removed)
			}
		}
	}
}
