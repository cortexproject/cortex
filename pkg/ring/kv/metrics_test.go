package kv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// operationAbortedError is a local stub that mimics ha.ReplicasNotMatchError,
// implementing IsOperationAborted() to avoid an import cycle.
type operationAbortedError struct{}

func (e operationAbortedError) Error() string            { return "operation aborted" }
func (e operationAbortedError) IsOperationAborted() bool { return true }

func TestGetCasErrorCode(t *testing.T) {
	abortedErr := operationAbortedError{}

	tests := map[string]struct {
		err      error
		expected string
	}{
		"nil error": {
			err:      nil,
			expected: "200",
		},
		"operation aborted error (direct)": {
			err:      abortedErr,
			expected: "200",
		},
		"operation aborted error (single-wrapped by memberlist)": {
			err:      fmt.Errorf("fn returned error: %w", abortedErr),
			expected: "200",
		},
		"operation aborted error (double-wrapped by memberlist)": {
			err: fmt.Errorf("failed to CAS-update key X: %w",
				fmt.Errorf("fn returned error: %w", abortedErr)),
			expected: "200",
		},
		"generic error": {
			err:      fmt.Errorf("some real error"),
			expected: "500",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, getCasErrorCode(tc.err))
		})
	}
}
