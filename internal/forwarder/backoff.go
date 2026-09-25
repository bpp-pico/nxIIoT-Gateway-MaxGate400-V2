package forwarder

import "time"

// BackoffDuration implements the exponential retry schedule from §9.4:
// 1, 2, 4, 8, 16, 32, 60, 60, ... seconds. retryCount is the attempt number
// (1 = first failure).
func BackoffDuration(retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	const maxBackoff = 60 * time.Second

	// 2^(retryCount-1) seconds, capped at 60s. Compute via shift on a small
	// bound to avoid overflow for pathologically large retry counts.
	shift := retryCount - 1
	if shift > 6 { // 2^6 = 64 already exceeds the 60s cap
		return maxBackoff
	}
	d := time.Duration(1<<uint(shift)) * time.Second
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}
