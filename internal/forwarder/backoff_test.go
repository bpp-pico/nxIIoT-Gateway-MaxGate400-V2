package forwarder

import (
	"testing"
	"time"
)

// TestBackoffDurationMatchesDesignDocSequence pins the exact sequence from
// §9.4: 1, 2, 4, 8, 16, 32, 60, 60, ... seconds.
func TestBackoffDurationMatchesDesignDocSequence(t *testing.T) {
	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second,
		60 * time.Second,
		60 * time.Second, // stays capped for further retries
	}

	for i, w := range want {
		retryCount := i + 1
		got := BackoffDuration(retryCount)
		if got != w {
			t.Errorf("BackoffDuration(%d) = %v, want %v", retryCount, got, w)
		}
	}
}

func TestBackoffDurationClampsBelowOne(t *testing.T) {
	if got := BackoffDuration(0); got != 1*time.Second {
		t.Errorf("BackoffDuration(0) = %v, want 1s", got)
	}
	if got := BackoffDuration(-5); got != 1*time.Second {
		t.Errorf("BackoffDuration(-5) = %v, want 1s", got)
	}
}
