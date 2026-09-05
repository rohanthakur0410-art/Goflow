package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Handler executes the work for a single job. Implementations must respect
// ctx cancellation/deadline promptly so that timeouts and manual
// cancellation behave correctly.
type Handler func(ctx context.Context, payload []byte) error

// Registry maps a job Type string to the Handler responsible for it. It is
// safe to read concurrently once built; registration happens once at
// startup, before workers begin dequeuing jobs.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry returns an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register adds (or replaces) the handler for a job type.
func (r *Registry) Register(jobType string, h Handler) {
	r.handlers[jobType] = h
}

// Lookup returns the handler for a job type, and whether it was found.
func (r *Registry) Lookup(jobType string) (Handler, bool) {
	h, ok := r.handlers[jobType]
	return h, ok
}

// Types returns the list of registered job types.
func (r *Registry) Types() []string {
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

// sleepWithContext sleeps for d, but returns ctx.Err() early if ctx is
// cancelled or its deadline passes first. All example handlers use this so
// they demonstrably honor cancellation and timeouts.
func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RegisterDefaultHandlers wires up the five example job types described in
// the README: email, report_generation, data_processing, image_processing,
// and webhook. Each is a small but genuine simulation of real work,
// including realistic failure modes, so the retry/timeout/dead-letter
// machinery has something meaningful to exercise.

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type reportPayload struct {
	ReportName string `json:"report_name"`
	Rows       int    `json:"rows"`
}

type dataProcessingPayload struct {
	Dataset   string `json:"dataset"`
	Operation string `json:"operation"`
}

type imageProcessingPayload struct {
	ImageURL string `json:"image_url"`
	Resize   string `json:"resize"`
}

type webhookPayload struct {
	URL    string `json:"url"`
	Method string `json:"method"`
}

func RegisterDefaultHandlers(r *Registry) {
	r.Register("email", emailHandler)
	r.Register("report_generation", reportGenerationHandler)
	r.Register("data_processing", dataProcessingHandler)
	r.Register("image_processing", imageProcessingHandler)
	r.Register("webhook", webhookHandler)
}

// emailHandler simulates sending an email. It validates the payload and
// "sends" by sleeping a short, realistic duration.
func emailHandler(ctx context.Context, payload []byte) error {
	var p emailPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid email payload: %w", err)
	}
	if p.To == "" {
		return fmt.Errorf("email payload missing 'to' address")
	}
	return sleepWithContext(ctx, 150*time.Millisecond)
}

// reportGenerationHandler simulates generating a report proportional to the
// requested row count, so larger reports realistically take longer (and are
// more likely to hit a timeout if one is configured too aggressively).
func reportGenerationHandler(ctx context.Context, payload []byte) error {
	var p reportPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid report_generation payload: %w", err)
	}
	if p.ReportName == "" {
		return fmt.Errorf("report_generation payload missing 'report_name'")
	}
	work := time.Duration(50+p.Rows) * time.Millisecond
	if work > 5*time.Second {
		work = 5 * time.Second
	}
	return sleepWithContext(ctx, work)
}

// dataProcessingHandler simulates a transform/aggregate pipeline step.
func dataProcessingHandler(ctx context.Context, payload []byte) error {
	var p dataProcessingPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid data_processing payload: %w", err)
	}
	if p.Dataset == "" || p.Operation == "" {
		return fmt.Errorf("data_processing payload requires 'dataset' and 'operation'")
	}
	// A deterministic checksum stands in for real transformation work.
	sum := sha256.Sum256(payload)
	_ = hex.EncodeToString(sum[:])
	return sleepWithContext(ctx, 200*time.Millisecond)
}

// imageProcessingHandler simulates a resize/transcode operation.
func imageProcessingHandler(ctx context.Context, payload []byte) error {
	var p imageProcessingPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid image_processing payload: %w", err)
	}
	if p.ImageURL == "" {
		return fmt.Errorf("image_processing payload missing 'image_url'")
	}
	return sleepWithContext(ctx, 300*time.Millisecond)
}

// webhookHandler simulates delivering a webhook call. URLs pointing at
// "fail.test" deterministically fail, which is useful for exercising retry
// behavior in tests and demos without depending on the network.
func webhookHandler(ctx context.Context, payload []byte) error {
	var p webhookPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid webhook payload: %w", err)
	}
	if p.URL == "" {
		return fmt.Errorf("webhook payload missing 'url'")
	}
	if err := sleepWithContext(ctx, 100*time.Millisecond); err != nil {
		return err
	}
	if containsFailTrigger(p.URL) {
		return fmt.Errorf("webhook delivery to %s failed: connection refused", p.URL)
	}
	return nil
}

func containsFailTrigger(url string) bool {
	for i := 0; i+len("fail.test") <= len(url); i++ {
		if url[i:i+len("fail.test")] == "fail.test" {
			return true
		}
	}
	return false
}
