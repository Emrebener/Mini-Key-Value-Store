package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Emrebener/Mini-Key-Value-Store/internal/server"
	"github.com/Emrebener/Mini-Key-Value-Store/internal/store"
)

type config struct {
	addr string
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
	addr := flag.String("addr", "127.0.0.1:11211", "TCP address to listen on")
	flag.Parse()
	return config{addr: *addr}
}

func run(parent context.Context, cfg config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	logger.Info("listening", "addr", listener.Addr().String())

	store := store.New()
	var wg sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

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
