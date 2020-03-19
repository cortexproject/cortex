package correctness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMinQueryTime(t *testing.T) {
	tests := []struct {
		cfg      RunnerConfig
		expected time.Time
	}{
		{
			cfg:      RunnerConfig{},
			expected: time.Now(),
		},
		{
			cfg: RunnerConfig{
				timeQueryStart: NewTimeValue(time.Unix(1234567890, 0)),
			},
			expected: time.Unix(1234567890, 0),
		},
		{
			cfg: RunnerConfig{
				durationQuerySince: 10 * time.Hour,
			},
			expected: time.Now().Add(-10 * time.Hour),
		},
	}

	for _, tt := range tests {
		assert.WithinDuration(t, tt.expected, tt.cfg.minQueryTime(), 5*time.Millisecond)
	}
}
