package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDecideTimeoutResponse_NilStats(t *testing.T) {
	cfg := PhaseTrackerConfig{
		EvalTimeThreshold: 45 * time.Second,
	}
	assert.Equal(t, Default5XX, DecideTimeoutResponse(nil, cfg))
}

func TestDecideTimeoutResponse_ZeroQueryStart(t *testing.T) {
	stats := &QueryStats{}
	// queryStart is zero by default (never set).
	cfg := PhaseTrackerConfig{
		EvalTimeThreshold: 45 * time.Second,
	}
	assert.Equal(t, Default5XX, DecideTimeoutResponse(stats, cfg))
}

func TestDecideTimeoutResponse_EvalBelowThreshold(t *testing.T) {
	stats := &QueryStats{}
	// Set queryStart to now so evalTime ≈ 0, well below threshold.
	stats.SetQueryStart(time.Now())
	// No storage wall time, so evalTime = time.Since(now) - 0 ≈ 0.

	cfg := PhaseTrackerConfig{
		EvalTimeThreshold: 45 * time.Second,
	}
	assert.Equal(t, Default5XX, DecideTimeoutResponse(stats, cfg))
}

func TestDecideTimeoutResponse_EvalAboveThreshold(t *testing.T) {
	stats := &QueryStats{}
	// Set queryStart 60s in the past so time.Since(queryStart) ≈ 60s.
	stats.SetQueryStart(time.Now().Add(-60 * time.Second))
	// Set storageWallTime to 5s so evalTime ≈ 60s - 5s = 55s > 45s threshold.
	stats.AddQueryStorageWallTime(5 * time.Second)

	cfg := PhaseTrackerConfig{
		EvalTimeThreshold: 45 * time.Second,
	}
	assert.Equal(t, UserError4XX, DecideTimeoutResponse(stats, cfg))
}
