package jobs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestTypeLimiter_UnlimitedTypeNeverBlocks(t *testing.T) {
	l := NewTypeLimiter(map[string]int{"capped": 1})
	for i := 0; i < 50; i++ {
		if err := l.Acquire(context.Background(), "uncapped"); err != nil {
			t.Fatalf("Acquire on unlimited type returned %v", err)
		}
	}
	// Release on an unlimited type must be a safe no-op, even with no
	// matching Acquires.
	l.Release("uncapped")
	if _, ok := l.Limit("uncapped"); ok {
		t.Fatal("Limit reports a cap for an unconfigured type")
	}
}

func TestTypeLimiter_EnforcesCap(t *testing.T) {
	l := NewTypeLimiter(map[string]int{"capped": 2})
	ctx := context.Background()

	if err := l.Acquire(ctx, "capped"); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if err := l.Acquire(ctx, "capped"); err != nil {
		t.Fatalf("second Acquire: %v", err)
	}

	// Third acquire must block; prove it with a short-timeout context.
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := l.Acquire(waitCtx, "capped"); err != context.DeadlineExceeded {
		t.Fatalf("third Acquire: got %v, want context.DeadlineExceeded", err)
	}

	// Releasing a slot must let exactly one more waiter in.
	l.Release("capped")
	if err := l.Acquire(ctx, "capped"); err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}

	waitCtx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel2()
	if err := l.Acquire(waitCtx2, "capped"); err != context.DeadlineExceeded {
		t.Fatalf("Acquire at cap again: got %v, want context.DeadlineExceeded", err)
	}
}

func TestTypeLimiter_ConcurrentAcquiresRespectCap(t *testing.T) {
	const cap = 3
	l := NewTypeLimiter(map[string]int{"capped": cap})
	var inFlight, maxSeen atomic.Int32

	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			if err := l.Acquire(context.Background(), "capped"); err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			cur := inFlight.Add(1)
			for {
				m := maxSeen.Load()
				if cur <= m || maxSeen.CompareAndSwap(m, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			l.Release("capped")
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
	if got := maxSeen.Load(); got > cap {
		t.Fatalf("max concurrent holders = %d, want <= %d", got, cap)
	}
}

func TestTypeLimiter_CancelledContextUnblocksWait(t *testing.T) {
	l := NewTypeLimiter(map[string]int{"capped": 1})
	ctx := context.Background()
	if err := l.Acquire(ctx, "capped"); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	waitCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- l.Acquire(waitCtx, "capped") }()

	cancel()
	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire did not return after context cancellation")
	}
}

func TestTypeLimiter_OverReleaseIsDropped(t *testing.T) {
	l := NewTypeLimiter(map[string]int{"capped": 1})
	// No Acquire at all: Release must not block or panic.
	l.Release("capped")
	if err := l.Acquire(context.Background(), "capped"); err != nil {
		t.Fatalf("Acquire after stray Release: %v", err)
	}
}
