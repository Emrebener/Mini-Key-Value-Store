package benchmark

import (
	"testing"
	"time"
)

func TestSummarizeComputesLatencyStatistics(t *testing.T) {
	summary := Summarize([]time.Duration{
		30 * time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
		20 * time.Millisecond,
	})

	if summary.Count != 4 {
		t.Fatalf("Count = %d, want 4", summary.Count)
	}
	if summary.Min != 10*time.Millisecond {
		t.Fatalf("Min = %s, want 10ms", summary.Min)
	}
	if summary.Mean != 40*time.Millisecond {
		t.Fatalf("Mean = %s, want 40ms", summary.Mean)
	}
	if summary.P50 != 20*time.Millisecond {
		t.Fatalf("P50 = %s, want 20ms", summary.P50)
	}
	if summary.P95 != 100*time.Millisecond {
		t.Fatalf("P95 = %s, want 100ms", summary.P95)
	}
	if summary.Max != 100*time.Millisecond {
		t.Fatalf("Max = %s, want 100ms", summary.Max)
	}
	if summary.OpsPerSec != 25 {
		t.Fatalf("OpsPerSec = %.2f, want 25", summary.OpsPerSec)
	}
}

func TestSummarizeHandlesNoSamples(t *testing.T) {
	summary := Summarize(nil)

	if summary.Count != 0 {
		t.Fatalf("Count = %d, want 0", summary.Count)
	}
	if summary.OpsPerSec != 0 {
		t.Fatalf("OpsPerSec = %.2f, want 0", summary.OpsPerSec)
	}
}
