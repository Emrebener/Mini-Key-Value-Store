package benchmark

import (
	"math"
	"sort"
	"time"
)

type Summary struct {
	Count     int
	Min       time.Duration
	Mean      time.Duration
	P50       time.Duration
	P95       time.Duration
	Max       time.Duration
	OpsPerSec float64
}

func Summarize(samples []time.Duration, wallClock time.Duration) Summary {
	if len(samples) == 0 {
		return Summary{}
	}

	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	var total time.Duration
	for _, sample := range sorted {
		total += sample
	}

	opsPerSec := 0.0
	if wallClock > 0 {
		opsPerSec = float64(len(sorted)) / wallClock.Seconds()
	}

	return Summary{
		Count:     len(sorted),
		Min:       sorted[0],
		Mean:      total / time.Duration(len(sorted)),
		P50:       percentile(sorted, 50),
		P95:       percentile(sorted, 95),
		Max:       sorted[len(sorted)-1],
		OpsPerSec: opsPerSec,
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	index := int(math.Ceil((p/100)*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
