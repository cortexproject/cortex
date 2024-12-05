package ruler

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestUserExternalLabels(t *testing.T) {
	limits := ruleLimits{}
	e := newUserExternalLabels(labels.FromStrings("from", "cortex"), &limits)

	tests := []struct {
		name                   string
		removeBeforeTest       bool
		exists                 bool
		userExternalLabels     labels.Labels
		expectedExternalLabels labels.Labels
	}{
		{
			name:                   "global labels only",
			removeBeforeTest:       false,
			exists:                 false,
			userExternalLabels:     nil,
			expectedExternalLabels: labels.FromStrings("from", "cortex"),
		},
		{
			name:                   "local labels without overriding",
			removeBeforeTest:       true,
			exists:                 false,
			userExternalLabels:     labels.FromStrings("tag", "local"),
			expectedExternalLabels: labels.FromStrings("from", "cortex", "tag", "local"),
		},
		{
			name:                   "local labels that override globals",
			removeBeforeTest:       false,
			exists:                 true,
			userExternalLabels:     labels.FromStrings("from", "cloud", "tag", "local"),
			expectedExternalLabels: labels.FromStrings("from", "cloud", "tag", "local"),
		},
	}

	const userID = "test-user"
	for _, data := range tests {
		data := data
		t.Run(data.name, func(t *testing.T) {
			if data.removeBeforeTest {
				e.remove(userID)
			}
			_, exists := e.get(userID)
			require.Equal(t, data.exists, exists)

			limits.externalLabels = data.userExternalLabels
			lset, ok := e.update(userID)
			require.True(t, ok)
			require.Equal(t, data.expectedExternalLabels, lset)
			lset1, ok := e.update(userID)
			require.False(t, ok) // Not updated.
			require.Equal(t, data.expectedExternalLabels, lset1)
		})
	}

	_, ok := e.get(userID)
	require.True(t, ok)
	e.cleanup()
	_, ok = e.get(userID)
	require.False(t, ok)
}
