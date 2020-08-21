package correctness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMinQueryTime(t *testing.T) {
	tests := []struct {
		durationQuerySince time.Duration
		timeQueryStart     TimeValue
		expected           time.Time
	}{
		{
			expected: time.Now(),
		},
		{
			timeQueryStart: NewTimeValue(time.Unix(1234567890, 0)),
			expected:       time.Unix(1234567890, 0),
		},
		{
			durationQuerySince: 10 * time.Hour,
			expected:           time.Now().Add(-10 * time.Hour),
		},
	}

	for _, tt := range tests {
		assert.WithinDuration(t, tt.expected, calculateMinQueryTime(tt.durationQuerySince, tt.timeQueryStart), 50*time.Millisecond)
	}
}
