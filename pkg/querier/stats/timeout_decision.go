package stats

import "time"

// TimeoutDecision represents the classification of a query timeout.
type TimeoutDecision int

const (
	// Default5XX means return 503 (current behavior).
	Default5XX TimeoutDecision = iota
	// UserError4XX means the query is too expensive, return 422.
	UserError4XX
)

// PhaseTrackerConfig holds configurable thresholds for timeout classification.
type PhaseTrackerConfig struct {
	// TotalTimeout is the total time before the querier cancels the query context.
	TotalTimeout time.Duration

	// EvalTimeThreshold is the eval time above which a timeout is classified as user error (4XX).
	EvalTimeThreshold time.Duration

	// Enabled controls whether the 5XX-to-4XX conversion is active.
	Enabled bool
}

// DecideTimeoutResponse inspects QueryStats phase timings and returns a TimeoutDecision.
// It returns UserError4XX if eval time exceeds the threshold, Default5XX otherwise.
// It is a pure function that does not modify stats or cfg.
func DecideTimeoutResponse(stats *QueryStats, cfg PhaseTrackerConfig) TimeoutDecision {
	if stats == nil {
		return Default5XX
	}

	queryStart := stats.LoadQueryStart()
	if queryStart.IsZero() {
		return Default5XX
	}

	evalTime := time.Since(queryStart) - stats.LoadQueryStorageWallTime()

	if evalTime > cfg.EvalTimeThreshold {
		return UserError4XX
	}

	return Default5XX
}
