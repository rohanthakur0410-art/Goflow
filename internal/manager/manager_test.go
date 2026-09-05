package manager

import (
	"errors"
	"testing"
	"time"

	"goflow/internal/jobs"
)

func TestBackoffFor(t *testing.T) {
	cases := []struct {
		retryCount int
		want       time.Duration
	}{
		{0, 0},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
	}
	for _, c := range cases {
		if got := backoffFor(c.retryCount); got != c.want {
			t.Errorf("backoffFor(%d) = %s, want %s", c.retryCount, got, c.want)
		}
	}
}

func TestSubmit_ValidationErrors(t *testing.T) {
	mgr := New(nil, nil, nil, nil, 3, 30*time.Second)

	_, err := mgr.Submit(SubmitInput{Type: "", Priority: jobs.PriorityHigh})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation for empty type, got %v", err)
	}

	_, err = mgr.Submit(SubmitInput{Type: "email", Priority: jobs.Priority("URGENT")})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation for invalid priority, got %v", err)
	}

	negRetries := -1
	_, err = mgr.Submit(SubmitInput{Type: "email", Priority: jobs.PriorityHigh, MaxRetries: &negRetries})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation for negative max_retries, got %v", err)
	}

	zeroTimeout := 0
	_, err = mgr.Submit(SubmitInput{Type: "email", Priority: jobs.PriorityHigh, TimeoutSecs: &zeroTimeout})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation for zero timeout, got %v", err)
	}
}
