package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Emrebener/Mini-Key-Value-Store/internal/benchmark"
)

type service struct {
	name string
	addr string
	dial func(context.Context, string, time.Duration) (benchmark.Client, error)
}

func main() {
	var (
		servicesFlag  = flag.String("services", "minikv,redis,memcached", "comma-separated services to benchmark: minikv,redis,memcached")
		minikvAddr    = flag.String("minikv-addr", "127.0.0.1:11211", "MiniKV TCP address")
		redisAddr     = flag.String("redis-addr", "127.0.0.1:6379", "Redis TCP address")
		memcachedAddr = flag.String("memcached-addr", "127.0.0.1:11212", "Memcached TCP address")
		runs          = flag.Int("runs", 5, "number of repeated write/read runs")
		keys          = flag.Int("keys", 1000, "number of keys per run")
		valueBytes    = flag.Int("value-bytes", 128, "fixed payload size in bytes")
		prefix        = flag.String("prefix", "minikv-bench", "key prefix")
		timeout       = flag.Duration("timeout", 3*time.Second, "per-operation network deadline")
		concurrency   = flag.Int("concurrency", 1, "parallel client connections per service")
	)
	flag.Parse()

	registry := map[string]service{
		"minikv":    {name: "minikv", addr: *minikvAddr, dial: benchmark.DialMiniKV},
		"redis":     {name: "redis", addr: *redisAddr, dial: benchmark.DialRedis},
		"memcached": {name: "memcached", addr: *memcachedAddr, dial: benchmark.DialMemcached},
	}
	selected, err := selectServices(*servicesFlag, registry)
	if err != nil {
		log.Fatal(err)
	}

	workload := benchmark.Workload{
		Runs:        *runs,
		Keys:        *keys,
		ValueBytes:  *valueBytes,
		KeyPrefix:   *prefix,
		Concurrency: *concurrency,
	}
	ctx := context.Background()

	fmt.Printf("service\tworkload\tcount\tmin\tmean\tp50\tp95\tmax\tops/sec\n")
	for _, svc := range selected {
		dial := func(ctx context.Context) (benchmark.Client, error) {
			return svc.dial(ctx, svc.addr, *timeout)
		}
		results, err := benchmark.Run(ctx, dial, workload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "benchmark %s: %v\n", svc.name, err)
			os.Exit(1)
		}
		for _, result := range results {
			printResult(result)
		}
	}
}

func selectServices(raw string, registry map[string]service) ([]service, error) {
	var selected []service
	for _, token := range strings.Split(raw, ",") {
		name := strings.TrimSpace(token)
		if name == "" {
			continue
		}
		svc, ok := registry[name]
		if !ok {
			return nil, fmt.Errorf("unknown service %q", name)
		}
		selected = append(selected, svc)
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("at least one service is required")
	}
	return selected, nil
}

func printResult(result benchmark.Result) {
	summary := result.Summary
	fmt.Printf(
		"%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%.2f\n",
		result.Service,
		result.Workload,
		summary.Count,
		formatDuration(summary.Min),
		formatDuration(summary.Mean),
		formatDuration(summary.P50),
		formatDuration(summary.P95),
		formatDuration(summary.Max),
		summary.OpsPerSec,
	)
}

func formatDuration(duration time.Duration) string {
	return fmt.Sprintf("%.3fms", float64(duration.Microseconds())/1000)
}
