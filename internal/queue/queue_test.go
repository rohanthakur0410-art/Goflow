package queue

import (
	"context"
	"testing"
	"time"
)

func TestQueue_PriorityOrdering(t *testing.T) {
	q := New()
	q.Push("low", 2)
	q.Push("high", 0)
	q.Push("medium", 1)

	want := []string{"high", "medium", "low"}
	for _, w := range want {
		id, ok := q.Pop(context.Background())
		if !ok {
			t.Fatalf("expected a value, queue reported empty")
		}
		if id != w {
			t.Fatalf("got %q, want %q", id, w)
		}
	}
}

func TestQueue_FIFOWithinSamePriority(t *testing.T) {
	q := New()
	q.Push("first", 1)
	q.Push("second", 1)
	q.Push("third", 1)

	for _, want := range []string{"first", "second", "third"} {
		id, ok := q.Pop(context.Background())
		if !ok || id != want {
			t.Fatalf("got %q ok=%v, want %q", id, ok, want)
		}
	}
}

func TestQueue_HighPriorityJumpsAheadOfExistingLow(t *testing.T) {
	q := New()
	q.Push("low-1", 2)
	q.Push("low-2", 2)
	q.Push("high-1", 0) // pushed last but should still come out first

	id, ok := q.Pop(context.Background())
	if !ok || id != "high-1" {
		t.Fatalf("got %q ok=%v, want high-1", id, ok)
	}
}

func TestQueue_PopBlocksUntilPush(t *testing.T) {
	q := New()
	resultCh := make(chan string, 1)

	go func() {
		id, ok := q.Pop(context.Background())
		if ok {
			resultCh <- id
		} else {
			resultCh <- "<closed>"
		}
	}()

	select {
	case <-resultCh:
		t.Fatal("Pop returned before any item was pushed")
	case <-time.After(100 * time.Millisecond):
		// expected: still blocked
	}

	q.Push("job-1", 1)

	select {
	case id := <-resultCh:
		if id != "job-1" {
			t.Fatalf("got %q, want job-1", id)
		}
	case <-time.After(time.Second):
		t.Fatal("Pop did not return after Push")
	}
}

func TestQueue_PopRespectsContextCancellation(t *testing.T) {
	q := New()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool, 1)
	go func() {
		_, ok := q.Pop(ctx)
		done <- ok
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("expected Pop to return ok=false after context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("Pop did not return after context cancellation")
	}
}

func TestQueue_CloseWakesBlockedPop(t *testing.T) {
	q := New()
	done := make(chan bool, 1)
	go func() {
		_, ok := q.Pop(context.Background())
		done <- ok
	}()

	time.Sleep(50 * time.Millisecond)
	q.Close()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("expected Pop to return ok=false after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Pop did not return after Close")
	}

	// Pushing after Close should be a silent no-op, not a panic.
	q.Push("ignored", 0)
	if q.Len() != 0 {
		t.Fatalf("expected Len()==0 after push-to-closed-queue, got %d", q.Len())
	}
}

func TestQueue_Len(t *testing.T) {
	q := New()
	if q.Len() != 0 {
		t.Fatalf("expected empty queue to have Len 0, got %d", q.Len())
	}
	q.Push("a", 1)
	q.Push("b", 1)
	if q.Len() != 2 {
		t.Fatalf("expected Len 2, got %d", q.Len())
	}
	q.Pop(context.Background())
	if q.Len() != 1 {
		t.Fatalf("expected Len 1 after one pop, got %d", q.Len())
	}
}

// TestQueue_ConcurrentProducersConsumers exercises the queue with many
// goroutines pushing and popping simultaneously, primarily to give the race
// detector a workout.
func TestQueue_ConcurrentProducersConsumers(t *testing.T) {
	q := New()
	const n = 200

	go func() {
		for i := 0; i < n; i++ {
			q.Push("job", i%3)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	received := 0
	for received < n {
		_, ok := q.Pop(ctx)
		if !ok {
			t.Fatalf("Pop failed before receiving all %d items (got %d)", n, received)
		}
		received++
	}
}
