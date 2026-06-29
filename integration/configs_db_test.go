//go:build integration_configs_db

package integration

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/configs/userconfig"
)

// configsDBSetup starts Postgres + a configs-target Cortex service, returning the
// scenario and the configs service. The caller can build a ConfigsClient against
// configs.HTTPEndpoint().
func configsDBSetup(t *testing.T) (*e2e.Scenario, *e2ecortex.CortexService) {
	t.Helper()

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	t.Cleanup(s.Close)

	postgres := e2edb.NewPostgres(e2edb.PostgresHostName)
	require.NoError(t, s.StartAndWaitReady(postgres))

	configs := e2ecortex.NewConfigs("configs", map[string]string{
		"-configs.database.uri": fmt.Sprintf(
			"postgres://%s@%s:%d/%s?sslmode=disable",
			e2edb.PostgresUser,
			e2e.NetworkContainerHost(networkName, e2edb.PostgresHostName),
			e2edb.PostgresPort,
			e2edb.PostgresDB,
		),
	}, "")
	require.NoError(t, s.StartAndWaitReady(configs))

	return s, configs
}

func makeUserID(prefix string, n int) string {
	return fmt.Sprintf("%s-%d", prefix, n)
}

func makeRulesConfig(payload string) userconfig.Config {
	return userconfig.Config{
		RulesConfig: userconfig.RulesConfig{
			FormatVersion: userconfig.RuleFormatV2,
			Files: map[string]string{
				"rules.yaml": payload,
			},
		},
	}
}

func makeAlertmanagerConfig(receiver string) userconfig.Config {
	return userconfig.Config{
		AlertmanagerConfig: fmt.Sprintf(`route:
  receiver: %s
receivers:
- name: %s
`, receiver, receiver),
		RulesConfig: userconfig.RulesConfig{FormatVersion: userconfig.RuleFormatV2},
	}
}

// TestConfigsDB_MigrationsRunOnStartup proves the configs service successfully
// runs the SQL migrations under cmd/cortex/migrations against an empty database.
// configsDBSetup already waits for the /ready endpoint, so reaching this point
// is the assertion.
func TestConfigsDB_MigrationsRunOnStartup(t *testing.T) {
	_, _ = configsDBSetup(t)
}

func TestConfigsDB_AnonymousReturns401(t *testing.T) {
	_, configs := configsDBSetup(t)

	client := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), "")
	_, code, err := client.GetRulesConfig(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, code)
}

func TestConfigsDB_GetMissingReturns404(t *testing.T) {
	_, configs := configsDBSetup(t)

	client := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), "user-missing")
	_, code, err := client.GetRulesConfig(context.Background())
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, code)
}

func TestConfigsDB_PostAndGetRulesConfig(t *testing.T) {
	_, configs := configsDBSetup(t)

	client := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), "tenant-rules")
	cfg := makeRulesConfig("groups: []")

	code, body, err := client.PostRulesConfig(context.Background(), cfg)
	require.NoError(t, err)
	require.Equalf(t, http.StatusNoContent, code, "unexpected status, body: %s", body)

	view, code, err := client.GetRulesConfig(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.NotNil(t, view)
	assert.Equal(t, cfg.RulesConfig.Files, view.Config.RulesConfig.Files)
}

func TestConfigsDB_PostAndGetAlertmanagerConfig(t *testing.T) {
	_, configs := configsDBSetup(t)

	client := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), "tenant-am")
	cfg := makeAlertmanagerConfig("dummy")

	code, body, err := client.PostAlertmanagerConfig(context.Background(), cfg)
	require.NoError(t, err)
	require.Equalf(t, http.StatusNoContent, code, "unexpected status, body: %s", body)

	view, code, err := client.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.NotNil(t, view)
	assert.Equal(t, cfg.AlertmanagerConfig, view.Config.AlertmanagerConfig)
}

func TestConfigsDB_MultiTenantIsolation(t *testing.T) {
	_, configs := configsDBSetup(t)

	tenants := []string{makeUserID("iso", 1), makeUserID("iso", 2)}
	configs1 := makeRulesConfig("groups: [{name: t1}]")
	configs2 := makeRulesConfig("groups: [{name: t2}]")

	c1 := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), tenants[0])
	c2 := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), tenants[1])

	code, body, err := c1.PostRulesConfig(context.Background(), configs1)
	require.NoError(t, err)
	require.Equalf(t, http.StatusNoContent, code, "unexpected status, body: %s", body)

	code, body, err = c2.PostRulesConfig(context.Background(), configs2)
	require.NoError(t, err)
	require.Equalf(t, http.StatusNoContent, code, "unexpected status, body: %s", body)

	view1, code, err := c1.GetRulesConfig(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, configs1.RulesConfig.Files, view1.Config.RulesConfig.Files)

	view2, code, err := c2.GetRulesConfig(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, configs2.RulesConfig.Files, view2.Config.RulesConfig.Files)
}

func TestConfigsDB_GetAllConfigsReturnsLatest(t *testing.T) {
	_, configs := configsDBSetup(t)

	tenant := makeUserID("latest", 1)
	client := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), tenant)

	older := makeRulesConfig("groups: [{name: older}]")
	newer := makeRulesConfig("groups: [{name: newer}]")
	for _, cfg := range []userconfig.Config{older, older, newer} {
		code, body, err := client.PostRulesConfig(context.Background(), cfg)
		require.NoError(t, err)
		require.Equalf(t, http.StatusNoContent, code, "unexpected status, body: %s", body)
	}

	admin := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), "")
	all, code, err := admin.GetAllRulesConfigs(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)

	view, ok := all[tenant]
	require.True(t, ok, "tenant %q not present in admin response", tenant)
	assert.Equal(t, newer.RulesConfig.Files, view.Config.RulesConfig.Files)
}

// TestConfigsDB_GetAllConfigsEmpty proves the admin GetAllConfigs SQL returns an
// empty result set (not an error) against a freshly-migrated, empty database.
func TestConfigsDB_GetAllConfigsEmpty(t *testing.T) {
	_, configs := configsDBSetup(t)

	admin := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), "")
	all, code, err := admin.GetAllRulesConfigs(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	assert.Empty(t, all)
}

// TestConfigsDB_PostAnonymousReturns401 covers the write path's tenant-auth check,
// mirroring the read-path check in TestConfigsDB_AnonymousReturns401.
func TestConfigsDB_PostAnonymousReturns401(t *testing.T) {
	_, configs := configsDBSetup(t)

	client := e2ecortex.NewConfigsClient(configs.HTTPEndpoint(), "")
	code, _, err := client.PostRulesConfig(context.Background(), makeRulesConfig("groups: []"))
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, code)
}

// TestConfigsDB_GetConfigsSince proves the GetConfigs(since) SQL filter returns
// only configs whose version ID is greater than the supplied cursor. This is the
// one Postgres-specific query path not exercised by the other tests.
func TestConfigsDB_GetConfigsSince(t *testing.T) {
	_, configs := configsDBSetup(t)
	ctx := context.Background()
	endpoint := configs.HTTPEndpoint()

	// Post one config per tenant, in order, capturing each assigned version ID.
	post := func(tenant, payload string) userconfig.ID {
		c := e2ecortex.NewConfigsClient(endpoint, tenant)
		code, body, err := c.PostRulesConfig(ctx, makeRulesConfig(payload))
		require.NoError(t, err)
		require.Equalf(t, http.StatusNoContent, code, "unexpected status, body: %s", body)
		view, code, err := c.GetRulesConfig(ctx)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, code)
		return view.ID
	}

	user1, user2, user3 := makeUserID("since", 1), makeUserID("since", 2), makeUserID("since", 3)
	post(user1, "groups: [{name: first}]")
	id2 := post(user2, "groups: [{name: second}]")
	post(user3, "groups: [{name: third}]")

	// since=id2 must exclude user1 and user2 (IDs <= id2) and include only user3.
	admin := e2ecortex.NewConfigsClient(endpoint, "")
	all, code, err := admin.GetAllRulesConfigsSince(ctx, id2)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)

	_, has1 := all[user1]
	_, has2 := all[user2]
	_, has3 := all[user3]
	assert.False(t, has1, "user1 (ID < since) should be excluded")
	assert.False(t, has2, "user2 (ID == since) should be excluded")
	assert.True(t, has3, "user3 (ID > since) should be included")
}
