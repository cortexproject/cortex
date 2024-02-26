package tsdb

import (
	"flag"
	"time"
)

// CircuitBreakerConfig is the config for the circuit breaker.
type CircuitBreakerConfig struct {
	// Enabled enables circuit breaker.
	Enabled bool `yaml:"enabled"`

	// HalfOpenMaxRequests is the maximum number of requests allowed to pass through
	// when the circuit breaker is half-open.
	// If set to 0, the circuit breaker allows only 1 request.
	HalfOpenMaxRequests uint `yaml:"half_open_max_requests"`
	// OpenDuration is the period of the open state after which the state of the circuit breaker becomes half-open.
	// If set to 0, the circuit breaker utilizes the default value of 60 seconds.
	OpenDuration time.Duration `yaml:"open_duration"`
	// MinRequests is minimal requests to trigger the circuit breaker.
	MinRequests uint `yaml:"min_requests"`
	// ConsecutiveFailures represents consecutive failures based on CircuitBreakerMinRequests to determine if the circuit breaker should open.
	ConsecutiveFailures uint `yaml:"consecutive_failures"`
	// FailurePercent represents the failure percentage, which is based on CircuitBreakerMinRequests, to determine if the circuit breaker should open.
	FailurePercent float64 `yaml:"failure_percent"`
}

func (cfg *CircuitBreakerConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.Enabled, prefix+"circuit-breaker.enabled", false, "If true, enable circuit breaker.")
	f.UintVar(&cfg.HalfOpenMaxRequests, prefix+"circuit-breaker.half-open-max-requests", 10, "Maximum number of requests allowed to pass through when the circuit breaker is half-open. If set to 0, by default it allows 1 request.")
	f.DurationVar(&cfg.OpenDuration, prefix+"circuit-breaker.open-duration", 5*time.Second, "Period of the open state after which the state of the circuit breaker becomes half-open. If set to 0, by default open duration is 60 seconds.")
	f.UintVar(&cfg.MinRequests, prefix+"circuit-breaker.min-requests", 50, "Minimal requests to trigger the circuit breaker.")
	f.UintVar(&cfg.ConsecutiveFailures, prefix+"circuit-breaker.consecutive-failures", 5, "Consecutive failures to determine if the circuit breaker should open.")
	f.Float64Var(&cfg.FailurePercent, prefix+"circuit-breaker.failure-percent", 0.05, "Failure percentage to determine if the circuit breaker should open.")
}
