package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"time"
)

type Workload struct {
	Runs       int
	Keys       int
	ValueBytes int
	KeyPrefix  string
}

type Result struct {
	Service  string
	Workload string
	Summary  Summary
}

func Run(ctx context.Context, client Client, workload Workload) ([]Result, error) {
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

	payload := bytes.Repeat([]byte("x"), workload.ValueBytes)
	writes := make([]time.Duration, 0, workload.Runs*workload.Keys)
	reads := make([]time.Duration, 0, workload.Runs*workload.Keys)

	for run := 0; run < workload.Runs; run++ {
		for i := 0; i < workload.Keys; i++ {
			key := workloadKey(workload.KeyPrefix, run, i)
			start := time.Now()
			if err := client.Set(ctx, key, payload); err != nil {
				return nil, fmt.Errorf("%s write run %d key %d: %w", client.Name(), run+1, i+1, err)
			}
			writes = append(writes, time.Since(start))
		}

		for i := 0; i < workload.Keys; i++ {
			key := workloadKey(workload.KeyPrefix, run, i)
			start := time.Now()
			value, ok, err := client.Get(ctx, key)
			elapsed := time.Since(start)
			if err != nil {
				return nil, fmt.Errorf("%s read run %d key %d: %w", client.Name(), run+1, i+1, err)
			}
			if !ok {
				return nil, fmt.Errorf("%s read run %d key %d: key missing", client.Name(), run+1, i+1)
			}
			if !bytes.Equal(value, payload) {
				return nil, fmt.Errorf("%s read run %d key %d: value mismatch", client.Name(), run+1, i+1)
			}
			reads = append(reads, elapsed)
		}
	}

	return []Result{
		{Service: client.Name(), Workload: "write", Summary: Summarize(writes)},
		{Service: client.Name(), Workload: "read", Summary: Summarize(reads)},
	}, nil
}

func workloadKey(prefix string, run int, index int) string {
	return fmt.Sprintf("%s:%d:%d", prefix, run, index)
}
