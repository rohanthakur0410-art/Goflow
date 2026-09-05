// Package queue implements the in-memory priority queue that holds
// job IDs ready for immediate execution. It is deliberately independent of
// any external broker (no Redis, no Kafka): a container/heap plus a mutex
// and a condition variable is all that's needed at this scale, and it keeps
// the concurrency story easy to reason about and explain.
package queue

import (
	"container/heap"
	"context"
	"sync"
)

// Item is a single entry in the priority queue.
type Item struct {
	JobID  string
	Weight int   // lower = higher priority (see jobs.Priority.Weight)
	seq    int64 // insertion sequence, used as a tie-breaker for FIFO order
	index  int   // maintained by container/heap
}

// innerHeap implements heap.Interface. Not safe for concurrent use on its
// own; always accessed through Queue's mutex.
type innerHeap []*Item

func (h innerHeap) Len() int { return len(h) }
func (h innerHeap) Less(i, j int) bool {
	if h[i].Weight != h[j].Weight {
		return h[i].Weight < h[j].Weight
	}
	return h[i].seq < h[j].seq // earlier-enqueued item wins ties
}
func (h innerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *innerHeap) Push(x any) {
	item := x.(*Item)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *innerHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

// Queue is a thread-safe, blocking priority queue of job IDs.
type Queue struct {
	mu      sync.Mutex
	notify  *sync.Cond
	heap    innerHeap
	nextSeq int64
	closed  bool
}

// New returns an empty, ready-to-use Queue.
func New() *Queue {
	q := &Queue{heap: make(innerHeap, 0)}
	q.notify = sync.NewCond(&q.mu)
	return q
}

// Push adds a job ID to the queue at the given priority weight. Safe to call
// from any goroutine (API handlers, the scheduler, or the retry timer).
func (q *Queue) Push(jobID string, weight int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.nextSeq++
	heap.Push(&q.heap, &Item{JobID: jobID, Weight: weight, seq: q.nextSeq})
	q.notify.Signal()
}

// Pop blocks until a job is available, the queue is closed, or ctx is
// cancelled. It returns ok=false in the latter two cases.
func (q *Queue) Pop(ctx context.Context) (jobID string, ok bool) {
	// A watcher goroutine turns ctx cancellation into a Broadcast so the
	// blocked Cond.Wait() below can wake up even though sync.Cond has no
	// native context support.
	done := make(chan struct{})
	if ctx != nil {
		stop := context.AfterFunc(ctx, func() {
			q.mu.Lock()
			q.notify.Broadcast()
			q.mu.Unlock()
		})
		defer func() {
			stop()
			close(done)
		}()
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.heap) == 0 && !q.closed {
		if ctx != nil && ctx.Err() != nil {
			return "", false
		}
		q.notify.Wait()
	}
	if len(q.heap) == 0 {
		return "", false
	}
	item := heap.Pop(&q.heap).(*Item)
	return item.JobID, true
}

// Len returns the current number of queued (not-yet-dequeued) jobs.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.heap)
}

// Close wakes up every blocked Pop call; subsequent Push calls are no-ops.
// Used during graceful shutdown.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.notify.Broadcast()
}
