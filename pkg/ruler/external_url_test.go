package ruler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUserExternalURL(t *testing.T) {
	limits := ruleLimits{}
	e := newUserExternalURL("http://global:9090", &limits)

	const userID = "test-user"

	t.Run("global URL used when no per-tenant override", func(t *testing.T) {
		e.remove(userID)
		url, changed := e.update(userID)
		require.True(t, changed)
		require.Equal(t, "http://global:9090", url)
	})

	t.Run("no change on second update", func(t *testing.T) {
		url, changed := e.update(userID)
		require.False(t, changed)
		require.Equal(t, "http://global:9090", url)
	})

	t.Run("per-tenant URL overrides global", func(t *testing.T) {
		limits.mtx.Lock()
		limits.externalURL = "http://tenant:3000"
		limits.mtx.Unlock()

		url, changed := e.update(userID)
		require.True(t, changed)
		require.Equal(t, "http://tenant:3000", url)
	})

	t.Run("no change when per-tenant URL is the same", func(t *testing.T) {
		url, changed := e.update(userID)
		require.False(t, changed)
		require.Equal(t, "http://tenant:3000", url)
	})

	t.Run("revert to global when per-tenant override removed", func(t *testing.T) {
		limits.mtx.Lock()
		limits.externalURL = ""
		limits.mtx.Unlock()

		url, changed := e.update(userID)
		require.True(t, changed)
		require.Equal(t, "http://global:9090", url)
	})

	t.Run("remove and cleanup lifecycle", func(t *testing.T) {
		e.remove(userID)
		// After remove, next update should report changed
		url, changed := e.update(userID)
		require.True(t, changed)
		require.Equal(t, "http://global:9090", url)

		e.cleanup()
		// After cleanup, next update should report changed
		url, changed = e.update(userID)
		require.True(t, changed)
		require.Equal(t, "http://global:9090", url)
	})
}
