package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActiveSeriesTrackerConfig_Validate(t *testing.T) {
	t.Run("valid regex", func(t *testing.T) {
		cfg := ActiveSeriesTrackerConfig{Name: "test", Matchers: `{__name__=~"api_.*"}`}
		require.NoError(t, cfg.Validate())
		assert.Len(t, cfg.ParsedMatchers(), 1)
	})

	t.Run("invalid regex", func(t *testing.T) {
		cfg := ActiveSeriesTrackerConfig{Name: "test", Matchers: `{__name__=~"[bad"}`}
		assert.Error(t, cfg.Validate())
	})

	t.Run("empty matchers", func(t *testing.T) {
		cfg := ActiveSeriesTrackerConfig{Name: "test", Matchers: ``}
		assert.Error(t, cfg.Validate())
	})

	t.Run("empty name", func(t *testing.T) {
		cfg := ActiveSeriesTrackerConfig{Name: "", Matchers: `{__name__="foo"}`}
		assert.ErrorIs(t, cfg.Validate(), errActiveSeriesTrackerEmptyName)
	})
}

func TestActiveSeriesTrackersConfig_Validate(t *testing.T) {
	t.Run("all valid", func(t *testing.T) {
		cfg := ActiveSeriesTrackersConfig{
			{Name: "a", Matchers: `{__name__="foo"}`},
			{Name: "b", Matchers: `{__name__=~"bar.*"}`},
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("one invalid fails all", func(t *testing.T) {
		cfg := ActiveSeriesTrackersConfig{
			{Name: "a", Matchers: `{__name__="foo"}`},
			{Name: "bad", Matchers: `{__name__=~"[bad"}`},
		}
		assert.Error(t, cfg.Validate())
	})

	t.Run("nil is valid", func(t *testing.T) {
		var cfg ActiveSeriesTrackersConfig
		require.NoError(t, cfg.Validate())
	})

	t.Run("duplicate names", func(t *testing.T) {
		cfg := ActiveSeriesTrackersConfig{
			{Name: "dup", Matchers: `{__name__="foo"}`},
			{Name: "dup", Matchers: `{__name__="bar"}`},
		}
		assert.ErrorIs(t, cfg.Validate(), errActiveSeriesTrackerDuplicateName)
	})

	t.Run("empty name in list", func(t *testing.T) {
		cfg := ActiveSeriesTrackersConfig{
			{Name: "", Matchers: `{__name__="foo"}`},
		}
		assert.ErrorIs(t, cfg.Validate(), errActiveSeriesTrackerEmptyName)
	})
}

func TestOverrides_ActiveSeriesTrackers_PerTenant(t *testing.T) {
	defaults := Limits{}
	defaults.ActiveSeriesTrackers = ActiveSeriesTrackersConfig{
		{Name: "default_tracker", Matchers: `{__name__=~".*"}`},
	}
	require.NoError(t, defaults.ActiveSeriesTrackers.Validate())

	SetDefaultLimitsForYAMLUnmarshalling(defaults)

	tenantTrackers := ActiveSeriesTrackersConfig{
		{Name: "tenant_api", Matchers: `{__name__=~"api_.*"}`},
		{Name: "tenant_node", Matchers: `{__name__=~"node_.*"}`},
	}
	require.NoError(t, tenantTrackers.Validate())

	tenantLimits := &Limits{}
	*tenantLimits = defaults
	tenantLimits.ActiveSeriesTrackers = tenantTrackers

	overrides := NewOverrides(defaults, newMockTenantLimits(map[string]*Limits{
		"tenant-with-override": tenantLimits,
	}))

	// Tenant with override gets tenant-specific trackers.
	trackers := overrides.ActiveSeriesTrackers("tenant-with-override")
	require.Len(t, trackers, 2)
	assert.Equal(t, "tenant_api", trackers[0].Name)
	assert.Equal(t, "tenant_node", trackers[1].Name)

	// Tenant without override gets default trackers.
	trackers = overrides.ActiveSeriesTrackers("tenant-without-override")
	require.Len(t, trackers, 1)
	assert.Equal(t, "default_tracker", trackers[0].Name)
}

func TestOverrides_ActiveSeriesTrackers_NoDefaults(t *testing.T) {
	defaults := Limits{}
	overrides := NewOverrides(defaults, nil)

	trackers := overrides.ActiveSeriesTrackers("any-tenant")
	assert.Empty(t, trackers)
}
