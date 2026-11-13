package manager

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"goflow/internal/jobs"
	"goflow/internal/validation"
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

func TestSubmit_PayloadSchemaViolation(t *testing.T) {
	dir := t.TempDir()
	schema := `{
	  "$schema": "https://json-schema.org/draft/2020-12/schema",
	  "type": "object",
	  "required": ["to"],
	  "properties": { "to": { "type": "string", "minLength": 1 } }
	}`
	if err := os.WriteFile(filepath.Join(dir, "email.json"), []byte(schema), 0o644); err != nil {
		t.Fatalf("writing schema: %v", err)
	}
	v, err := validation.LoadDir(dir)
	if err != nil {
		t.Fatalf("loading schemas: %v", err)
	}

	mgr := New(nil, nil, nil, nil, 3, 30*time.Second)
	mgr.SetValidator(v)

	// Violation is rejected as a validation error before the (nil) store is
	// ever touched.
	_, err = mgr.Submit(SubmitInput{Type: "email", Payload: []byte(`{"subject": "hi"}`), Priority: jobs.PriorityHigh})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation for schema-violating payload, got %v", err)
	}
	if !strings.Contains(err.Error(), "to") {
		t.Fatalf("error should name the missing field, got %q", err.Error())
	}
}
