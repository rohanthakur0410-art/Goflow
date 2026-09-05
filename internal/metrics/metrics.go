// Package metrics implements a tiny, dependency-free Prometheus exposition
// endpoint. GoFlow intentionally avoids the official client_golang library
// here: the metric surface is small (nine series) and a ~150-line file using
// only sync/atomic gives the same scrape-able /metrics output without
// pulling in a dependency tree for it.
package metrics

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// Metrics holds every counter/gauge GoFlow exposes. All fields are safe for
// concurrent use.
type Metrics struct {
	jobsSubmitted  atomic.Int64
	jobsCompleted  atomic.Int64
	jobsFailed     atomic.Int64
	jobsRetried    atomic.Int64
	jobsCancelled  atomic.Int64
	workerFailures atomic.Int64

	queueSize     atomic.Int64
	activeWorkers atomic.Int64

	durationMu    sync.Mutex
	durationCount int64
	durationSumMS float64
	// durationBuckets holds cumulative-friendly bucket boundaries (ms) for a
	// simple Prometheus histogram of job processing duration.
	durationBuckets []float64
	durationCounts  []int64
}

var defaultBuckets = []float64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000}

// New returns a ready-to-use metrics registry.
func New() *Metrics {
	return &Metrics{
		durationBuckets: defaultBuckets,
		durationCounts:  make([]int64, len(defaultBuckets)),
	}
}

func (m *Metrics) IncJobsSubmitted()  { m.jobsSubmitted.Add(1) }
func (m *Metrics) IncJobsCompleted()  { m.jobsCompleted.Add(1) }
func (m *Metrics) IncJobsFailed()     { m.jobsFailed.Add(1) }
func (m *Metrics) IncJobsRetried()    { m.jobsRetried.Add(1) }
func (m *Metrics) IncJobsCancelled()  { m.jobsCancelled.Add(1) }
func (m *Metrics) IncWorkerFailures() { m.workerFailures.Add(1) }

func (m *Metrics) SetQueueSize(n int)     { m.queueSize.Store(int64(n)) }
func (m *Metrics) SetActiveWorkers(n int) { m.activeWorkers.Store(int64(n)) }

// ObserveJobDuration records the processing duration (in milliseconds) of a
// finished job attempt into the histogram.
func (m *Metrics) ObserveJobDuration(ms float64) {
	m.durationMu.Lock()
	defer m.durationMu.Unlock()
	m.durationCount++
	m.durationSumMS += ms
	for i, bound := range m.durationBuckets {
		if ms <= bound {
			m.durationCounts[i]++
		}
	}
}

// Render writes the current state of all metrics in Prometheus text
// exposition format (version 0.0.4).
func (m *Metrics) Render() string {
	var b strings.Builder

	writeCounter(&b, "goflow_jobs_submitted_total", "Total jobs submitted via the API", m.jobsSubmitted.Load())
	writeCounter(&b, "goflow_jobs_completed_total", "Total jobs that completed successfully", m.jobsCompleted.Load())
	writeCounter(&b, "goflow_jobs_failed_total", "Total job attempts that failed", m.jobsFailed.Load())
	writeCounter(&b, "goflow_jobs_retried_total", "Total job retry attempts scheduled", m.jobsRetried.Load())
	writeCounter(&b, "goflow_jobs_cancelled_total", "Total jobs cancelled by users", m.jobsCancelled.Load())
	writeCounter(&b, "goflow_worker_failures_total", "Total worker-side panics/failures recovered", m.workerFailures.Load())

	writeGauge(&b, "goflow_queue_size", "Current number of jobs waiting in the in-memory priority queue", float64(m.queueSize.Load()))
	writeGauge(&b, "goflow_active_workers", "Current number of running worker goroutines", float64(m.activeWorkers.Load()))

	m.durationMu.Lock()
	count := m.durationCount
	sum := m.durationSumMS
	counts := append([]int64(nil), m.durationCounts...)
	bounds := m.durationBuckets
	m.durationMu.Unlock()

	fmt.Fprintf(&b, "# HELP goflow_job_duration_milliseconds Job processing duration in milliseconds\n")
	fmt.Fprintf(&b, "# TYPE goflow_job_duration_milliseconds histogram\n")
	for i, bound := range bounds {
		fmt.Fprintf(&b, "goflow_job_duration_milliseconds_bucket{le=\"%s\"} %d\n", trimFloat(bound), counts[i])
	}
	fmt.Fprintf(&b, "goflow_job_duration_milliseconds_bucket{le=\"+Inf\"} %d\n", count)
	fmt.Fprintf(&b, "goflow_job_duration_milliseconds_sum %s\n", trimFloat(sum))
	fmt.Fprintf(&b, "goflow_job_duration_milliseconds_count %d\n", count)

	return b.String()
}

func writeCounter(b *strings.Builder, name, help string, value int64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	fmt.Fprintf(b, "%s %d\n", name, value)
}

func writeGauge(b *strings.Builder, name, help string, value float64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s gauge\n", name)
	fmt.Fprintf(b, "%s %s\n", name, trimFloat(value))
}

func trimFloat(f float64) string {
	s := fmt.Sprintf("%.3f", f)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" || s == "-" {
		s = "0"
	}
	return s
}

// Snapshot is a plain-data view of current metric values, used by the
// dashboard's JSON stats endpoint so the frontend doesn't need to parse the
// Prometheus text format.
type Snapshot struct {
	JobsSubmitted int64 `json:"jobs_submitted"`
	JobsCompleted int64 `json:"jobs_completed"`
	JobsFailed    int64 `json:"jobs_failed"`
	JobsRetried   int64 `json:"jobs_retried"`
	JobsCancelled int64 `json:"jobs_cancelled"`
	QueueSize     int64 `json:"queue_size"`
	ActiveWorkers int64 `json:"active_workers"`
}

func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		JobsSubmitted: m.jobsSubmitted.Load(),
		JobsCompleted: m.jobsCompleted.Load(),
		JobsFailed:    m.jobsFailed.Load(),
		JobsRetried:   m.jobsRetried.Load(),
		JobsCancelled: m.jobsCancelled.Load(),
		QueueSize:     m.queueSize.Load(),
		ActiveWorkers: m.activeWorkers.Load(),
	}
}
