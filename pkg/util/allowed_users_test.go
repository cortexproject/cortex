package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllowedUsers_NoConfig(t *testing.T) {
	a := NewAllowedUsers(nil, nil)
	require.True(t, a.IsAllowed("all"))
	require.True(t, a.IsAllowed("users"))
	require.True(t, a.IsAllowed("allowed"))
}

func TestAllowedUsers_Enabled(t *testing.T) {
	a := NewAllowedUsers([]string{"A", "B"}, nil)
	require.True(t, a.IsAllowed("A"))
	require.True(t, a.IsAllowed("B"))
	require.False(t, a.IsAllowed("C"))
	require.False(t, a.IsAllowed("D"))
}

func TestAllowedUsers_Disabled(t *testing.T) {
	a := NewAllowedUsers(nil, []string{"A", "B"})
	require.False(t, a.IsAllowed("A"))
	require.False(t, a.IsAllowed("B"))
	require.True(t, a.IsAllowed("C"))
	require.True(t, a.IsAllowed("D"))
}

func TestAllowedUsers_Combination(t *testing.T) {
	a := NewAllowedUsers([]string{"A", "B"}, []string{"B", "C"})
	require.True(t, a.IsAllowed("A"))  // enabled, and not disabled
	require.False(t, a.IsAllowed("B")) // enabled, but also disabled
	require.False(t, a.IsAllowed("C")) // disabled
	require.False(t, a.IsAllowed("D")) // not enabled
}
