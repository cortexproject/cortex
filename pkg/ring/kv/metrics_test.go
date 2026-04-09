package kv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/ha"
)

func TestGetCasErrorCode(t *testing.T) {
	replicasNotMatch := ha.ReplicasNotMatchError{} // IsOperationAborted() = true

	tests := map[string]struct {
		err      error
		expected string
	}{
		"nil error": {
			err:      nil,
			expected: "200",
		},
		"ReplicasNotMatchError (direct)": {
			err:      replicasNotMatch,
			expected: "200",
		},
		"ReplicasNotMatchError (single-wrapped by memberlist)": {
			err:      fmt.Errorf("fn returned error: %w", replicasNotMatch),
			expected: "200",
		},
		"ReplicasNotMatchError (double-wrapped by memberlist)": {
			err: fmt.Errorf("failed to CAS-update key X: %w",
				fmt.Errorf("fn returned error: %w", replicasNotMatch)),
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
