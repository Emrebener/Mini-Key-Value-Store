package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

type Workload struct {
	Runs        int
	Keys        int
	ValueBytes  int
	KeyPrefix   string
	Concurrency int
}

type Result struct {
	Service  string
	Workload string
	Summary  Summary
}

type Dialer func(ctx context.Context) (Client, error)

func Run(ctx context.Context, dial Dialer, workload Workload) ([]Result, error) {
	if workload.Runs <= 0 {
		return nil, fmt.Errorf("runs must be positive")
	}
	if workload.Keys <= 0 {
		return nil, fmt.Errorf("keys must be positive")
	}
	if workload.ValueBytes <= 0 {
		return nil, fmt.Errorf("value bytes must be positive")
	}
	if workload.KeyPrefix == "" {
		workload.KeyPrefix = "minikv-bench"
	}
	if workload.Concurrency <= 0 {
		workload.Concurrency = 1
	}
	if workload.Concurrency > workload.Keys {
		return nil, fmt.Errorf("concurrency %d exceeds keys %d", workload.Concurrency, workload.Keys)
	}

	clients, err := dialAll(ctx, dial, workload.Concurrency)
	if err != nil {
		return nil, err
	}
	defer closeAll(clients)

	payload := bytes.Repeat([]byte("x"), workload.ValueBytes)
	totalOps := workload.Runs * workload.Keys

	writes := make([]time.Duration, 0, totalOps)
	reads := make([]time.Duration, 0, totalOps)

	var writeWall, readWall time.Duration
	for run := 0; run < workload.Runs; run++ {
		writeRunStart := time.Now()
		runWrites, err := runPhase(ctx, clients, workload, run, payload, phaseWrite)
		if err != nil {
			return nil, err
		}
		writeWall += time.Since(writeRunStart)
		writes = append(writes, runWrites...)

		readRunStart := time.Now()
		runReads, err := runPhase(ctx, clients, workload, run, payload, phaseRead)
		if err != nil {
			return nil, err
		}
		readWall += time.Since(readRunStart)
		reads = append(reads, runReads...)
	}

	name := clients[0].Name()
	return []Result{
		{Service: name, Workload: "write", Summary: Summarize(writes, writeWall)},
		{Service: name, Workload: "read", Summary: Summarize(reads, readWall)},
	}, nil
}

type phase int

const (
	phaseWrite phase = iota
	phaseRead
)

func runPhase(ctx context.Context, clients []Client, workload Workload, run int, payload []byte, p phase) ([]time.Duration, error) {
	concurrency := len(clients)
	perWorker := make([][]time.Duration, concurrency)
	errs := make([]error, concurrency)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		start, end := workerSlice(workload.Keys, concurrency, w)
		latencies := make([]time.Duration, 0, end-start)
		go func(w, start, end int, latencies []time.Duration) {
			defer wg.Done()
			client := clients[w]
			for i := start; i < end; i++ {
				key := workloadKey(workload.KeyPrefix, run, i)
				before := time.Now()
				switch p {
				case phaseWrite:
					if err := client.Set(ctx, key, payload); err != nil {
						errs[w] = fmt.Errorf("%s write run %d key %d: %w", client.Name(), run+1, i+1, err)
						return
					}
				case phaseRead:
					value, ok, err := client.Get(ctx, key)
					if err != nil {
						errs[w] = fmt.Errorf("%s read run %d key %d: %w", client.Name(), run+1, i+1, err)
						return
					}
					if !ok {
						errs[w] = fmt.Errorf("%s read run %d key %d: key missing", client.Name(), run+1, i+1)
						return
					}
					if !bytes.Equal(value, payload) {
						errs[w] = fmt.Errorf("%s read run %d key %d: value mismatch", client.Name(), run+1, i+1)
						return
					}
				}
				latencies = append(latencies, time.Since(before))
			}
			perWorker[w] = latencies
		}(w, start, end, latencies)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	merged := make([]time.Duration, 0, workload.Keys)
	for _, slice := range perWorker {
		merged = append(merged, slice...)
	}
	return merged, nil
}

func workerSlice(total, concurrency, worker int) (int, int) {
	base := total / concurrency
	extra := total % concurrency
	start := worker*base + min(worker, extra)
	width := base
	if worker < extra {
		width++
	}
	return start, start + width
}

func dialAll(ctx context.Context, dial Dialer, n int) ([]Client, error) {
	clients := make([]Client, 0, n)
	for i := 0; i < n; i++ {
		client, err := dial(ctx)
		if err != nil {
			closeAll(clients)
			return nil, fmt.Errorf("dial worker %d: %w", i, err)
		}
		clients = append(clients, client)
	}
	return clients, nil
}

func closeAll(clients []Client) {
	for _, client := range clients {
		_ = client.Close()
	}
}

func workloadKey(prefix string, run int, index int) string {
	return fmt.Sprintf("%s:%d:%d", prefix, run, index)
}
