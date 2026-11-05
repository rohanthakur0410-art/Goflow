package config

import (
	"os"
	"testing"
)

func TestParseScalingThresholds(t *testing.T) {
	rules, err := parseScalingThresholds("50:5,10:2,999999:10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(rules))
	}
	// Must be sorted ascending by QueueSize regardless of input order.
	if rules[0].QueueSize != 10 || rules[1].QueueSize != 50 || rules[2].QueueSize != 999999 {
		t.Fatalf("rules not sorted ascending: %+v", rules)
	}
}

func TestParseScalingThresholds_InvalidFormat(t *testing.T) {
	if _, err := parseScalingThresholds("not-a-rule"); err == nil {
		t.Fatal("expected an error for malformed threshold string")
	}
	if _, err := parseScalingThresholds("10:abc"); err == nil {
		t.Fatal("expected an error for non-numeric worker count")
	}
	if _, err := parseScalingThresholds(""); err == nil {
		t.Fatal("expected an error for empty threshold string")
	}
}

func TestConfig_WorkersForQueueSize(t *testing.T) {
	cfg := &Config{
		ScalingRules: []ScalingRule{
			{QueueSize: 10, WorkerCount: 2},
			{QueueSize: 50, WorkerCount: 5},
			{QueueSize: 999999, WorkerCount: 10},
		},
	}

	cases := []struct {
		queueSize int
		want      int
	}{
		{0, 2},
		{10, 2},
		{11, 5},
		{50, 5},
		{51, 10},
		{1000, 10},
	}
	for _, c := range cases {
		if got := cfg.WorkersForQueueSize(c.queueSize); got != c.want {
			t.Errorf("WorkersForQueueSize(%d) = %d, want %d", c.queueSize, got, c.want)
		}
	}
}

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port == "" || cfg.DatabasePath == "" || cfg.WorkerCount < 1 {
		t.Fatalf("unexpected default config: %+v", cfg)
	}
}

func writeDotEnv(t *testing.T, content string) string {
	t.Helper()
	p := t.TempDir() + "/.env"
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDotEnv_MissingFile(t *testing.T) {
	if err := loadDotEnv(t.TempDir() + "/does-not-exist"); err == nil {
		t.Fatal("expected an error for a missing file (callers ignore it), got nil")
	}
}

func TestLoadDotEnv_AppliesValues(t *testing.T) {
	p := writeDotEnv(t, "# comment\n\nPORT=9090\nexport WORKER_COUNT=7\nQUOTED=\"hello world\"\nSINGLE='x'\nmalformed line without equals\n=novalue\n")
	for _, k := range []string{"PORT", "WORKER_COUNT", "QUOTED", "SINGLE"} {
		os.Unsetenv(k)
	}
	if err := loadDotEnv(p); err != nil {
		t.Fatal(err)
	}
	defer os.Unsetenv("PORT")
	defer os.Unsetenv("WORKER_COUNT")
	defer os.Unsetenv("QUOTED")
	defer os.Unsetenv("SINGLE")
	if got := os.Getenv("PORT"); got != "9090" {
		t.Fatalf("PORT = %q, want 9090", got)
	}
	if got := os.Getenv("WORKER_COUNT"); got != "7" {
		t.Fatalf("WORKER_COUNT = %q, want 7 (export prefix)", got)
	}
	if got := os.Getenv("QUOTED"); got != "hello world" {
		t.Fatalf("QUOTED = %q, want quotes stripped", got)
	}
	if got := os.Getenv("SINGLE"); got != "x" {
		t.Fatalf("SINGLE = %q, want x", got)
	}
}

func TestLoadDotEnv_RealEnvWins(t *testing.T) {
	t.Setenv("PORT", "1234")
	p := writeDotEnv(t, "PORT=9090\n")
	if err := loadDotEnv(p); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("PORT"); got != "1234" {
		t.Fatalf("PORT = %q, want 1234 (real env must win over .env)", got)
	}
}
