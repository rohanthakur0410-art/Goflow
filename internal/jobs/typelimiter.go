package jobs

import (
	"context"
	"sync"
)

// TypeLimiter caps how many jobs of a given type may execute at the same
// time, independent of the overall worker count. It exists for job types
// that thrash a scarce downstream resource (a rate-limited API, a single
// GPU, a database that hates concurrent writes) even when the pool itself
// could happily run more of them.
//
// A type with no configured limit is never delayed: Acquire returns nil
// immediately and Release is a no-op, so unlimited operation costs one map
// read per job.
type TypeLimiter struct {
	// limits is written once by NewTypeLimiter and only read afterwards, so
	// it needs no lock. sem entries are created lazily under mu.
	limits map[string]int
	mu     sync.Mutex
	sem    map[string]chan struct{}
}

// NewTypeLimiter builds a limiter from a type -> max-concurrent map. Limits
// below 1 (or a missing entry) mean unlimited; config validation rejects
// them before this point, so this is purely defensive.
func NewTypeLimiter(limits map[string]int) *TypeLimiter {
	l := &TypeLimiter{limits: make(map[string]int, len(limits)), sem: make(map[string]chan struct{})}
	for jobType, n := range limits {
		if n > 0 {
			l.limits[jobType] = n
		}
	}
	return l
}

// Acquire blocks until a slot for jobType is free or ctx is done. It
// returns nil when the type is unlimited or the caller now holds a slot;
// on a nil return for a limited type the caller MUST call Release(jobType)
// exactly once. It returns ctx.Err() when the wait was interrupted (job
// timeout or manual cancellation).
func (l *TypeLimiter) Acquire(ctx context.Context, jobType string) error {
	sem := l.slot(jobType)
	if sem == nil {
		return nil
	}
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns a slot previously taken by Acquire. It is a no-op for
// unlimited types. An over-release (more Releases than Acquires) is dropped
// instead of blocking or panicking: the invariants that would make that
// possible are enforced by the single Acquire/Release pair in the worker's
// processJob path, and a stuck Release must never wedge a worker.
func (l *TypeLimiter) Release(jobType string) {
	sem := l.slot(jobType)
	if sem == nil {
		return
	}
	select {
	case <-sem:
	default:
	}
}

// Limit returns the configured cap for a job type and whether one exists.
func (l *TypeLimiter) Limit(jobType string) (int, bool) {
	n, ok := l.limits[jobType]
	return n, ok
}

// slot returns the semaphore channel for a limited type, creating it on
// first use, or nil for an unlimited type.
func (l *TypeLimiter) slot(jobType string) chan struct{} {
	n, ok := l.limits[jobType]
	if !ok {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	sem, ok := l.sem[jobType]
	if !ok {
		sem = make(chan struct{}, n)
		l.sem[jobType] = sem
	}
	return sem
}
