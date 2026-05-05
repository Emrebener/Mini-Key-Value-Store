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
	_ "net/http/pprof"
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
		go servePprof(ctx, cfg.PprofAddr, logger)
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

func servePprof(ctx context.Context, addr string, logger *slog.Logger) {
	server := &http.Server{Addr: addr}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	logger.Info("pprof listening", "addr", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Warn("pprof server stopped", "error", err)
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
