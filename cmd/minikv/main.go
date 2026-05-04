package main

import (
	"context"
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

	"github.com/Emrebener/Mini-Key-Value-Store/internal/server"
	"github.com/Emrebener/Mini-Key-Value-Store/internal/store"
)

type config struct {
	addr              string
	pprofAddr         string
	shards            int
	maxValueBytes     int
	maxMemoryBytes    int
	itemOverheadBytes int
	cleanupInterval   time.Duration
}

func main() {
	cfg := loadConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if err := run(context.Background(), cfg, logger); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func loadConfig() config {
	addr := flag.String("addr", "0.0.0.0:11211", "TCP address to listen on")
	pprofAddr := flag.String("pprof-addr", "", "HTTP address for net/http/pprof handlers; empty disables")
	shards := flag.Int("shards", store.DefaultConfig().Shards, "number of independently-locked shards in the store")
	maxValueBytes := flag.Int("max-value-bytes", store.DefaultConfig().MaxValueBytes, "maximum bytes allowed in one value")
	maxMemoryBytes := flag.Int("max-memory-bytes", store.DefaultConfig().MaxMemoryBytes, "maximum accounted key/value bytes before eviction")
	itemOverheadBytes := flag.Int("item-overhead-bytes", store.DefaultConfig().ItemOverheadBytes, "explicit per-item bytes included in memory accounting")
	cleanupInterval := flag.Duration("cleanup-interval", time.Minute, "interval for expired-key cleanup; set to 0 to disable")
	flag.Parse()
	return config{
		addr:              *addr,
		pprofAddr:         *pprofAddr,
		shards:            *shards,
		maxValueBytes:     *maxValueBytes,
		maxMemoryBytes:    *maxMemoryBytes,
		itemOverheadBytes: *itemOverheadBytes,
		cleanupInterval:   *cleanupInterval,
	}
}

func run(parent context.Context, cfg config, logger *slog.Logger) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	logger.Info("listening", "addr", listener.Addr().String())

	store := store.New(store.Config{
		MaxValueBytes:     cfg.maxValueBytes,
		MaxMemoryBytes:    cfg.maxMemoryBytes,
		ItemOverheadBytes: cfg.itemOverheadBytes,
		Shards:            cfg.shards,
	})
	var wg sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	go cleanupExpired(ctx, store, cfg.cleanupInterval, logger)

	if cfg.pprofAddr != "" {
		go servePprof(ctx, cfg.pprofAddr, logger)
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
			if err := server.ServeConn(conn, store); err != nil {
				logger.Warn("connection closed with error", "remote", conn.RemoteAddr().String(), "error", err)
			}
		}()
	}
}

func validateConfig(cfg config) error {
	if cfg.maxValueBytes <= 0 {
		return fmt.Errorf("max-value-bytes must be positive")
	}
	if cfg.maxMemoryBytes <= 0 {
		return fmt.Errorf("max-memory-bytes must be positive")
	}
	if cfg.itemOverheadBytes < 0 {
		return fmt.Errorf("item-overhead-bytes must be non-negative")
	}
	if cfg.maxValueBytes > cfg.maxMemoryBytes {
		return fmt.Errorf("max-value-bytes must be less than or equal to max-memory-bytes")
	}
	if cfg.cleanupInterval < 0 {
		return fmt.Errorf("cleanup-interval must be non-negative")
	}
	if cfg.shards <= 0 {
		return fmt.Errorf("shards must be positive")
	}
	return nil
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
