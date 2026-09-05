package jobs

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := NewRegistry()
	called := false
	r.Register("noop", func(ctx context.Context, payload []byte) error {
		called = true
		return nil
	})

	h, ok := r.Lookup("noop")
	if !ok {
		t.Fatal("expected handler to be found")
	}
	if err := h(context.Background(), nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("handler was not actually invoked")
	}

	if _, ok := r.Lookup("does_not_exist"); ok {
		t.Fatal("expected lookup of unregistered type to fail")
	}
}

func TestDefaultHandlers_Registered(t *testing.T) {
	r := NewRegistry()
	RegisterDefaultHandlers(r)

	for _, want := range []string{"email", "report_generation", "data_processing", "image_processing", "webhook"} {
		if _, ok := r.Lookup(want); !ok {
			t.Errorf("expected default handler %q to be registered", want)
		}
	}
}

func TestEmailHandler_ValidatesPayload(t *testing.T) {
	if err := emailHandler(context.Background(), []byte(`{"subject":"hi"}`)); err == nil {
		t.Fatal("expected error for missing 'to' field")
	}
	if err := emailHandler(context.Background(), []byte(`not json`)); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	payload, _ := json.Marshal(emailPayload{To: "a@b.com", Subject: "hi", Body: "hello"})
	if err := emailHandler(context.Background(), payload); err != nil {
		t.Fatalf("unexpected error for valid payload: %v", err)
	}
}

func TestWebhookHandler_DeterministicFailure(t *testing.T) {
	failPayload, _ := json.Marshal(webhookPayload{URL: "http://fail.test/hook", Method: "POST"})
	if err := webhookHandler(context.Background(), failPayload); err == nil {
		t.Fatal("expected fail.test URL to deterministically fail")
	}

	okPayload, _ := json.Marshal(webhookPayload{URL: "http://example.com/hook", Method: "POST"})
	if err := webhookHandler(context.Background(), okPayload); err != nil {
		t.Fatalf("unexpected error for normal URL: %v", err)
	}
}

func TestHandlers_RespectContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	payload, _ := json.Marshal(emailPayload{To: "a@b.com"})
	start := time.Now()
	err := emailHandler(ctx, payload)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected handler to return an error for a cancelled context")
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("handler took %s to notice cancellation, want near-instant", elapsed)
	}
}

func TestHandlers_RespectDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// report_generation sleeps proportional to `rows`; make it long enough
	// to guarantee the 10ms deadline fires first.
	payload, _ := json.Marshal(reportPayload{ReportName: "big", Rows: 5000})
	start := time.Now()
	err := reportGenerationHandler(ctx, payload)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("handler took %s to respect the deadline, way too slow", elapsed)
	}
}

func TestPriority_Weight(t *testing.T) {
	if PriorityHigh.Weight() >= PriorityMedium.Weight() {
		t.Error("HIGH should weigh less than MEDIUM")
	}
	if PriorityMedium.Weight() >= PriorityLow.Weight() {
		t.Error("MEDIUM should weigh less than LOW")
	}
}

func TestPriority_Valid(t *testing.T) {
	for _, p := range []Priority{PriorityHigh, PriorityMedium, PriorityLow} {
		if !p.Valid() {
			t.Errorf("expected %q to be valid", p)
		}
	}
	if Priority("URGENT").Valid() {
		t.Error("expected an unknown priority to be invalid")
	}
}

func TestStatus_IsTerminal(t *testing.T) {
	terminal := []Status{StatusCompleted, StatusCancelled, StatusDead}
	nonTerminal := []Status{StatusPending, StatusScheduled, StatusRunning, StatusFailed, StatusRetrying}

	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("expected %q to be terminal", s)
		}
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("expected %q to be non-terminal", s)
		}
	}
}

func TestNewID_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := NewID()
		if seen[id] {
			t.Fatalf("duplicate ID generated: %s", id)
		}
		seen[id] = true
	}
}
