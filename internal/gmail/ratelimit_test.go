package gmail

import (
	"context"
	"sync"
	"testing"
	"time"
)

// newTestLimiter creates a rate limiter with standard test defaults (5 QPS).
func newTestLimiter() *RateLimiter {
	return NewRateLimiter(5.0)
}

// drainBucket empties the rate limiter's token bucket.
func drainBucket(rl *RateLimiter) {
	for rl.TryAcquire(OpMessagesBatchDelete) {
	}
	for rl.TryAcquire(OpMessagesGet) {
	}
}

// getRefillRate safely reads the refill rate under the mutex.
func getRefillRate(rl *RateLimiter) float64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.refillRate
}

// getThrottledUntil safely reads the throttledUntil field under the mutex.
func getThrottledUntil(rl *RateLimiter) time.Time {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.throttledUntil
}

// assertRefillRate checks that the limiter's refill rate matches the expected value.
func assertRefillRate(t *testing.T, rl *RateLimiter, want float64, msg string) {
	t.Helper()
	got := getRefillRate(rl)
	if got != want {
		t.Errorf("%s: refillRate = %v, want %v", msg, got, want)
	}
}

// assertElapsedAtLeast checks that elapsed time meets a minimum threshold.
func assertElapsedAtLeast(t *testing.T, elapsed, min time.Duration, label string) {
	t.Helper()
	if elapsed < min {
		t.Errorf("%s: elapsed = %v, expected >= %v", label, elapsed, min)
	}
}

func TestOperationCost(t *testing.T) {
	tests := []struct {
		op   Operation
		cost int
	}{
		{OpMessagesGet, 5},
		{OpMessagesGetRaw, 5},
		{OpMessagesList, 5},
		{OpLabelsList, 1},
		{OpHistoryList, 2},
		{OpMessagesTrash, 5},
		{OpMessagesDelete, 10},
		{OpMessagesBatchDelete, 50},
		{OpProfile, 1},
		{Operation(999), 1}, // Unknown operation defaults to 1
	}

	for _, tc := range tests {
		got := tc.op.Cost()
		if got != tc.cost {
			t.Errorf("Operation(%d).Cost() = %d, want %d", tc.op, got, tc.cost)
		}
	}
}

func TestNewRateLimiter(t *testing.T) {
	rl := NewRateLimiter(5.0)

	if rl.capacity != DefaultCapacity {
		t.Errorf("capacity = %v, want %v", rl.capacity, DefaultCapacity)
	}

	if rl.tokens != DefaultCapacity {
		t.Errorf("initial tokens = %v, want %v", rl.tokens, DefaultCapacity)
	}

	// At 5 QPS, refill rate should be DefaultRefillRate
	if rl.refillRate != DefaultRefillRate {
		t.Errorf("refillRate = %v, want %v", rl.refillRate, DefaultRefillRate)
	}
}

func TestNewRateLimiter_ScaledQPS(t *testing.T) {
	// Lower QPS should scale refill rate
	rl := NewRateLimiter(2.5)
	expectedRate := DefaultRefillRate * 0.5 // 2.5/5.0 = 0.5
	if rl.refillRate != expectedRate {
		t.Errorf("refillRate at 2.5 QPS = %v, want %v", rl.refillRate, expectedRate)
	}

	// Higher QPS should cap at default (no increase above 5 QPS)
	rl = NewRateLimiter(10.0)
	if rl.refillRate != DefaultRefillRate {
		t.Errorf("refillRate at 10 QPS = %v, want %v (capped)", rl.refillRate, DefaultRefillRate)
	}
}

func TestRateLimiter_TryAcquire(t *testing.T) {
	rl := newTestLimiter()

	// Should succeed for small operations when bucket is full
	if !rl.TryAcquire(OpProfile) {
		t.Error("TryAcquire(OpProfile) should succeed when bucket is full")
	}

	// Drain the bucket
	drainBucket(rl)

	// Should fail for large operations immediately after draining
	// (even with fast refill, 50 units takes 200ms to refill)
	if rl.TryAcquire(OpMessagesBatchDelete) {
		t.Error("TryAcquire(OpMessagesBatchDelete) should fail when bucket is low")
	}
}

func TestRateLimiter_Acquire_Success(t *testing.T) {
	rl := newTestLimiter()

	// Use a generous timeout - success means completing within the timeout
	// This avoids flaky failures from CI load or GC pauses
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Should succeed quickly when bucket is full (fast path)
	start := time.Now()
	err := rl.Acquire(ctx, OpProfile)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Acquire() error = %v", err)
	}

	// Verify fast path is taken - should complete quickly
	// Use a generous threshold (500ms) to avoid flaky failures on slow CI runners
	// while still catching regressions that add seconds of wait time
	if elapsed > 500*time.Millisecond {
		t.Errorf("Acquire() took %v, expected fast path (<500ms)", elapsed)
	}
}

func TestRateLimiter_Acquire_ContextCancelled(t *testing.T) {
	rl := newTestLimiter()

	// Drain the bucket
	drainBucket(rl)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Acquire should return context error immediately
	err := rl.Acquire(ctx, OpMessagesGet)
	if err != context.Canceled {
		t.Errorf("Acquire() with cancelled context = %v, want context.Canceled", err)
	}
}

func TestRateLimiter_Acquire_ContextTimeout(t *testing.T) {
	// Use minimum QPS to slow refill significantly
	rl := NewRateLimiter(MinQPS)

	// Drain the bucket completely
	drainBucket(rl)

	// Request a large operation that will take longer than our timeout to refill
	timeout := 50 * time.Millisecond

	// Record start BEFORE creating context to ensure elapsed >= timeout
	start := time.Now()
	deadline := start.Add(timeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	err := rl.Acquire(ctx, OpMessagesBatchDelete) // Needs 50 tokens
	elapsed := time.Since(start)

	if err != context.DeadlineExceeded {
		t.Errorf("Acquire() with timeout = %v, want context.DeadlineExceeded", err)
	}

	// Should have waited at least the timeout duration (with small slack for scheduling)
	// (no upper bound assertion to avoid flaky tests under CI load)
	assertElapsedAtLeast(t, elapsed, timeout-5*time.Millisecond, "Acquire() with timeout")
}

func TestRateLimiter_Refill(t *testing.T) {
	rl := newTestLimiter()

	// Drain tokens
	drainBucket(rl)

	initial := rl.Available()
	if initial >= 50 {
		t.Fatalf("expected drained bucket, got %v tokens", initial)
	}

	// Wait for refill
	time.Sleep(50 * time.Millisecond)

	after := rl.Available()
	if after <= initial {
		t.Errorf("tokens should increase after waiting: %v -> %v", initial, after)
	}
}

func TestRateLimiter_Available(t *testing.T) {
	rl := newTestLimiter()

	initial := rl.Available()
	if initial != DefaultCapacity {
		t.Errorf("Available() = %v, want %v", initial, DefaultCapacity)
	}

	rl.TryAcquire(OpMessagesGet) // cost 5

	after := rl.Available()
	// Allow small delta for time-based refill
	expectedMin := DefaultCapacity - 5 - 1
	if after < float64(expectedMin) {
		t.Errorf("Available() after acquire = %v, want >= %v", after, expectedMin)
	}
}

func TestRateLimiter_Concurrent(t *testing.T) {
	rl := newTestLimiter()

	// Use a timeout to prevent test from hanging if Acquire blocks
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run many concurrent acquires
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rl.Acquire(ctx, OpProfile); err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent Acquire() error = %v", err)
	}
}

func TestRateLimiter_CapacityLimit(t *testing.T) {
	rl := newTestLimiter()

	// Wait a bit to let refill happen
	time.Sleep(20 * time.Millisecond)

	// Available should not exceed capacity
	avail := rl.Available()
	if avail > float64(DefaultCapacity) {
		t.Errorf("Available() = %v, should not exceed capacity %v", avail, DefaultCapacity)
	}
}

func TestRateLimiter_Throttle(t *testing.T) {
	rl := newTestLimiter()

	// Verify initial state
	initial := rl.Available()
	if initial != DefaultCapacity {
		t.Fatalf("initial Available() = %v, want %v", initial, DefaultCapacity)
	}

	// Throttle for a short duration
	rl.Throttle(100 * time.Millisecond)

	// Tokens should be drained immediately
	afterThrottle := rl.Available()
	if afterThrottle != 0 {
		t.Errorf("Available() after Throttle = %v, want 0", afterThrottle)
	}

	// Wait less than throttle duration - tokens should still be 0
	time.Sleep(30 * time.Millisecond)
	duringThrottle := rl.Available()
	if duringThrottle > 10 { // Allow small margin
		t.Errorf("Available() during throttle = %v, expected ~0", duringThrottle)
	}

	// Wait for throttle to expire
	time.Sleep(100 * time.Millisecond)

	// Tokens should start refilling again
	afterExpiry := rl.Available()
	if afterExpiry <= 0 {
		t.Errorf("Available() after throttle expiry = %v, expected >0", afterExpiry)
	}
}

func TestRateLimiter_RecoverRate(t *testing.T) {
	rl := newTestLimiter()

	// Throttle reduces rate to 50%
	rl.Throttle(10 * time.Millisecond)

	// Check rate was reduced
	assertRefillRate(t, rl, DefaultRefillRate*0.5, "after Throttle")

	// Recover the rate
	rl.RecoverRate()

	assertRefillRate(t, rl, DefaultRefillRate, "after RecoverRate")
}

func TestRateLimiter_Throttle_DoesNotShortenBackoff(t *testing.T) {
	rl := newTestLimiter()

	// Throttle for 200ms (simulating 403 quota error)
	rl.Throttle(200 * time.Millisecond)

	firstThrottleEnd := getThrottledUntil(rl)

	// Second throttle for 50ms (simulating 429 during backoff)
	// This should NOT shorten the existing 200ms backoff
	rl.Throttle(50 * time.Millisecond)

	secondThrottleEnd := getThrottledUntil(rl)

	if secondThrottleEnd.Before(firstThrottleEnd) {
		t.Errorf("Throttle shortened existing backoff: first=%v, second=%v",
			firstThrottleEnd, secondThrottleEnd)
	}
}

func TestRateLimiter_Throttle_ExtendsBackoff(t *testing.T) {
	rl := newTestLimiter()

	// Throttle for 50ms
	rl.Throttle(50 * time.Millisecond)

	firstThrottleEnd := getThrottledUntil(rl)

	// Wait 30ms, then throttle for another 50ms
	// This should extend the backoff since 30ms + 50ms > original 50ms
	time.Sleep(30 * time.Millisecond)
	rl.Throttle(50 * time.Millisecond)

	secondThrottleEnd := getThrottledUntil(rl)

	if !secondThrottleEnd.After(firstThrottleEnd) {
		t.Errorf("Throttle did not extend backoff: first=%v, second=%v",
			firstThrottleEnd, secondThrottleEnd)
	}
}

func TestRateLimiter_AutoRecoverRate(t *testing.T) {
	rl := newTestLimiter()

	// Throttle for a short duration
	rl.Throttle(50 * time.Millisecond)

	// Verify rate is reduced
	assertRefillRate(t, rl, DefaultRefillRate*0.5, "after Throttle")

	// Wait for throttle to expire (use generous margin for CI stability)
	time.Sleep(100 * time.Millisecond)

	// Call Available() which triggers refill and auto-recovery
	rl.Available()

	// Verify rate is restored
	assertRefillRate(t, rl, DefaultRefillRate, "after throttle expiry")
}

func TestRateLimiter_Acquire_WaitsForThrottle(t *testing.T) {
	rl := newTestLimiter()

	// Throttle for 100ms
	throttleDuration := 100 * time.Millisecond
	rl.Throttle(throttleDuration)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := rl.Acquire(ctx, OpProfile) // Small cost, should succeed after throttle
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Acquire() error = %v", err)
	}

	// Should have waited at least the throttle duration (with some tolerance)
	assertElapsedAtLeast(t, elapsed, throttleDuration-20*time.Millisecond, "Acquire() throttle wait")
}
