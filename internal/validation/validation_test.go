package validation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const emailSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["to"],
  "properties": {
    "to": { "type": "string", "minLength": 1 },
    "subject": { "type": "string" }
  }
}`

func writeSchema(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("writing schema: %v", err)
	}
}

func TestLoadDir_MissingDirDisablesValidation(t *testing.T) {
	v, err := LoadDir(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("missing dir should not error, got %v", err)
	}
	if len(v.Types()) != 0 {
		t.Fatalf("expected no schemas, got %v", v.Types())
	}
	if err := v.Validate("email", []byte(`{"anything": true}`)); err != nil {
		t.Fatalf("disabled validator should accept everything, got %v", err)
	}
}

func TestLoadDir_InvalidJSONFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "email.json", `{not json`)
	if _, err := LoadDir(dir); err == nil || !strings.Contains(err.Error(), "email.json") {
		t.Fatalf("expected error naming email.json, got %v", err)
	}
}

func TestLoadDir_InvalidSchemaFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "email.json", `{"type": "not-a-real-type"}`)
	if _, err := LoadDir(dir); err == nil || !strings.Contains(err.Error(), "email.json") {
		t.Fatalf("expected compile error naming email.json, got %v", err)
	}
}

func TestLoadDir_SkipsNonJSONFiles(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "email.json", emailSchema)
	writeSchema(t, dir, "README.txt", `not a schema`)
	v, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := v.Types(); len(got) != 1 || got[0] != "email" {
		t.Fatalf("expected [email], got %v", got)
	}
}

func TestValidate_UnknownTypePasses(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "email.json", emailSchema)
	v, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := v.Validate("webhook", []byte(`{"url": 42}`)); err != nil {
		t.Fatalf("type without schema should pass, got %v", err)
	}
}

func TestValidate_ValidPayloadPasses(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "email.json", emailSchema)
	v, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := v.Validate("email", []byte(`{"to": "a@b.c", "subject": "hi"}`)); err != nil {
		t.Fatalf("valid payload should pass, got %v", err)
	}
}

func TestValidate_MissingRequiredNamesField(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "email.json", emailSchema)
	v, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = v.Validate("email", []byte(`{"subject": "hi"}`))
	if err == nil {
		t.Fatal("missing required 'to' should fail")
	}
	if !strings.Contains(err.Error(), `"email"`) || !strings.Contains(err.Error(), "to") {
		t.Fatalf("error should name the type and field, got %q", err.Error())
	}
}

func TestValidate_WrongTypeNamesLocation(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "email.json", emailSchema)
	v, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = v.Validate("email", []byte(`{"to": 42}`))
	if err == nil {
		t.Fatal("non-string 'to' should fail")
	}
	if !strings.Contains(err.Error(), "/to") {
		t.Fatalf("error should locate /to, got %q", err.Error())
	}
}

func TestValidate_InvalidJSONPayload(t *testing.T) {
	dir := t.TempDir()
	writeSchema(t, dir, "email.json", emailSchema)
	v, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := v.Validate("email", []byte(`{oops`)); err == nil {
		t.Fatal("malformed JSON payload should fail")
	}
}

func TestValidate_ShippedExampleSchemas(t *testing.T) {
	// The schemas/ dir at the repo root documents the contracts of the five
	// example job types; make sure each compiles and rejects its canonical
	// bad payload.
	v, err := LoadDir(filepath.Join("..", "..", "schemas"))
	if err != nil {
		t.Fatalf("shipped schemas must compile: %v", err)
	}
	if len(v.Types()) != 5 {
		t.Fatalf("expected 5 shipped schemas, got %v", v.Types())
	}
	cases := []struct {
		jobType string
		good    string
		bad     string
	}{
		{"email", `{"to": "a@b.c"}`, `{"subject": "x"}`},
		{"report_generation", `{"report_name": "q3", "rows": 10}`, `{"report_name": "q3", "rows": -1}`},
		{"data_processing", `{"dataset": "d", "operation": "sum"}`, `{"dataset": "d"}`},
		{"image_processing", `{"image_url": "http://x/y.png"}`, `{}`},
		{"webhook", `{"url": "http://x", "method": "POST"}`, `{"url": "http://x", "method": "TELEPORT"}`},
	}
	for _, c := range cases {
		if err := v.Validate(c.jobType, []byte(c.good)); err != nil {
			t.Errorf("%s: good payload rejected: %v", c.jobType, err)
		}
		if err := v.Validate(c.jobType, []byte(c.bad)); err == nil {
			t.Errorf("%s: bad payload accepted", c.jobType)
		}
	}
}
