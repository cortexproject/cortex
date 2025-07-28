package querysharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/querysharding"
)

func TestDisableBinaryExpressionAnalyzer_Analyze(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		expectShardable bool
		expectError     bool
		description     string
	}{
		{
			name:            "binary expression with vector matching on",
			query:           `up{job="prometheus"} + on(instance) rate(cpu_usage[5m])`,
			expectShardable: true,
			expectError:     false,
			description:     "Binary expression with 'on' matching should remain shardable",
		},
		{
			name:            "binary expression without explicit vector matching",
			query:           `up{job="prometheus"} + rate(cpu_usage[5m])`,
			expectShardable: false,
			expectError:     false,
			description:     "No explicit vector matching means without. Not shardable.",
		},
		{
			name:            "binary expression with vector matching ignoring",
			query:           `up{job="prometheus"} + ignoring(instance) rate(cpu_usage[5m])`,
			expectShardable: false,
			expectError:     false,
			description:     "Binary expression with 'ignoring' matching should not be shardable",
		},
		{
			name:            "complex expression with binary expr using on",
			query:           `sum(rate(http_requests_total[5m])) by (job) + on(job) avg(cpu_usage) by (job)`,
			expectShardable: true,
			expectError:     false,
			description:     "Complex expression with 'on' matching should remain shardable",
		},
		{
			name:            "complex expression with binary expr using ignoring",
			query:           `sum(rate(http_requests_total[5m])) by (job) + ignoring(instance) avg(cpu_usage) by (job)`,
			expectShardable: false,
			expectError:     false,
			description:     "Complex expression with 'ignoring' matching should not be shardable",
		},
		{
			name:            "nested binary expressions with one ignoring",
			query:           `(up + on(job) rate(cpu[5m])) * ignoring(instance) memory_usage`,
			expectShardable: false,
			expectError:     false,
			description:     "Nested expressions with any 'ignoring' should not be shardable",
		},
		{
			name:            "aggregation",
			query:           `sum(rate(http_requests_total[5m])) by (job)`,
			expectShardable: true,
			expectError:     false,
			description:     "Aggregations should remain shardable",
		},
		{
			name:            "aggregation with binary expression and scalar",
			query:           `sum(rate(http_requests_total[5m])) by (job) * 100`,
			expectShardable: true,
			expectError:     false,
			description:     "Aggregations should remain shardable",
		},
		{
			name:            "invalid query",
			query:           "invalid{query",
			expectShardable: false,
			expectError:     true,
			description:     "Invalid queries should return error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the actual thanos analyzer
			thanosAnalyzer := querysharding.NewQueryAnalyzer()

			// Wrap it with our disable binary expression analyzer
			analyzer := NewDisableBinaryExpressionAnalyzer(thanosAnalyzer)

			// Test the wrapped analyzer
			result, err := analyzer.Analyze(tt.query)

			if tt.expectError {
				require.Error(t, err, tt.description)
				return
			}

			require.NoError(t, err, tt.description)
			assert.Equal(t, tt.expectShardable, result.IsShardable(), tt.description)
		})
	}
}

func TestDisableBinaryExpressionAnalyzer_ComparedToOriginal(t *testing.T) {
	// Test cases that verify the wrapper correctly modifies behavior
	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "ignoring expression should be disabled",
			query: `up + ignoring(instance) rate(cpu[5m])`,
		},
		{
			name:  "nested ignoring expression should be disabled",
			query: `(sum(rate(http_requests_total[5m])) by (job)) + ignoring(instance) avg(cpu_usage) by (job)`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test with original analyzer
			originalAnalyzer := querysharding.NewQueryAnalyzer()
			originalResult, err := originalAnalyzer.Analyze(tc.query)
			require.NoError(t, err)

			// Test with wrapped analyzer
			wrappedAnalyzer := NewDisableBinaryExpressionAnalyzer(originalAnalyzer)
			wrappedResult, err := wrappedAnalyzer.Analyze(tc.query)
			require.NoError(t, err)

			// The wrapped analyzer should make previously shardable queries non-shardable
			// if they contain binary expressions with ignoring
			if originalResult.IsShardable() {
				assert.False(t, wrappedResult.IsShardable(),
					"Wrapped analyzer should disable sharding for queries with ignoring vector matching")
			} else {
				// If original wasn't shardable, wrapped shouldn't be either
				assert.False(t, wrappedResult.IsShardable())
			}
		})
	}
}
