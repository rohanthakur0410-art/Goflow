package config

import "testing"

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
