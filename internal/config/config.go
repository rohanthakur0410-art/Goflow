// Package config loads GoFlow's runtime configuration from environment
// variables, applying sane local-development defaults for everything so the
// server can be started with zero configuration.
package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ScalingRule maps a queue-size threshold to the number of workers that
// should be running once the queue reaches that size.
type ScalingRule struct {
	QueueSize   int
	WorkerCount int
}

// Config holds every tunable of the GoFlow server.
type Config struct {
	Port         string
	DatabasePath string

	WorkerCount int // initial / minimum worker count
	QueueSize   int // buffered channel capacity for the internal job queue

	MaxRetries        int
	DefaultJobTimeout time.Duration
	ShutdownTimeout   time.Duration

	SchedulerPollInterval time.Duration
	ScalingCheckInterval  time.Duration
	AgingInterval         time.Duration // priority-aging tick; 0 disables aging
	HeartbeatInterval     time.Duration
	StaleWorkerThreshold  time.Duration

	ScalingRules []ScalingRule
}

// Load builds a Config from environment variables, falling back to defaults
// documented in .env.example.
func Load() (*Config, error) {
	cfg := &Config{
		Port:                  getEnv("PORT", "8080"),
		DatabasePath:          getEnv("DATABASE_PATH", "./data/goflow.db"),
		WorkerCount:           getEnvInt("WORKER_COUNT", 2),
		QueueSize:             getEnvInt("QUEUE_SIZE", 100),
		MaxRetries:            getEnvInt("MAX_RETRIES", 3),
		DefaultJobTimeout:     getEnvDuration("DEFAULT_JOB_TIMEOUT_SECONDS", 30*time.Second),
		ShutdownTimeout:       getEnvDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
		SchedulerPollInterval: getEnvDuration("SCHEDULER_POLL_INTERVAL_SECONDS", 2*time.Second),
		AgingInterval:         getEnvDuration("AGING_INTERVAL_SECONDS", 60*time.Second),
		ScalingCheckInterval:  getEnvDuration("SCALING_CHECK_INTERVAL_SECONDS", 3*time.Second),
		HeartbeatInterval:     getEnvDuration("HEARTBEAT_INTERVAL_SECONDS", 5*time.Second),
		StaleWorkerThreshold:  getEnvDuration("STALE_WORKER_THRESHOLD_SECONDS", 20*time.Second),
	}

	rules, err := parseScalingThresholds(getEnv("SCALING_THRESHOLDS", "10:2,50:5,999999:10"))
	if err != nil {
		return nil, fmt.Errorf("invalid SCALING_THRESHOLDS: %w", err)
	}
	cfg.ScalingRules = rules

	if cfg.WorkerCount < 1 {
		return nil, fmt.Errorf("WORKER_COUNT must be >= 1")
	}
	if cfg.MaxRetries < 0 {
		return nil, fmt.Errorf("MAX_RETRIES must be >= 0")
	}

	return cfg, nil
}

// WorkersForQueueSize returns how many workers should be running given the
// current queue depth, based on the configured scaling rules (ascending by
// QueueSize; the first rule whose threshold is >= the queue size wins).
func (c *Config) WorkersForQueueSize(queueSize int) int {
	for _, r := range c.ScalingRules {
		if queueSize <= r.QueueSize {
			return r.WorkerCount
		}
	}
	if len(c.ScalingRules) > 0 {
		return c.ScalingRules[len(c.ScalingRules)-1].WorkerCount
	}
	return c.WorkerCount
}

// parseScalingThresholds parses a string like "10:2,50:5,999999:10" into
// sorted ScalingRule entries.
func parseScalingThresholds(raw string) ([]ScalingRule, error) {
	parts := strings.Split(raw, ",")
	rules := make([]ScalingRule, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.Split(p, ":")
		if len(kv) != 2 {
			return nil, fmt.Errorf("expected queueSize:workerCount, got %q", p)
		}
		qs, err := strconv.Atoi(strings.TrimSpace(kv[0]))
		if err != nil {
			return nil, fmt.Errorf("bad queue size in %q: %w", p, err)
		}
		wc, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("bad worker count in %q: %w", p, err)
		}
		rules = append(rules, ScalingRule{QueueSize: qs, WorkerCount: wc})
	}
	if len(rules) == 0 {
		return nil, fmt.Errorf("no scaling rules provided")
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].QueueSize < rules[j].QueueSize })
	return rules, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return fallback
}
