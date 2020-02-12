package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/framework"
)

func TestAlertmanager(t *testing.T) {
	s, err := framework.NewScenario()
	require.NoError(t, err)
	defer s.Shutdown()

	AlertmanagerConfigs := map[string]string{
		"-alertmanager.storage.local.path": "/integration/alertmanager_test_fixtures/",
		"-alertmanager.storage.type":       "local",
		"-alertmanager.web.external-url":   "http://localhost/api/prom",
		"-log.level":                       "debug",
	}

	// Start Cortex components
	require.NoError(t, s.StartAlertmanager("alertmanager", AlertmanagerConfigs, ""))
	require.NoError(t, s.Service("alertmanager").WaitMetric(80, "cortex_alertmanager_configs", 1))

	c, err := framework.NewClient("", "", s.Endpoint("alertmanager", 80), "user-1")
	status, err := c.GetAlertmanagerStatus(context.Background())

	// Ensure the returned status config matches alertmanager_test_fixtures/user-1.yaml
	require.NotNil(t, status)
	require.Equal(t, "example_receiver", status.ConfigJSON.Route.Receiver)
	require.Len(t, status.ConfigJSON.Route.GroupByStr, 1)
	require.Equal(t, "example_groupby", status.ConfigJSON.Route.GroupByStr[0])
	require.Len(t, status.ConfigJSON.Receivers, 1)
	require.Equal(t, "example_receiver", status.ConfigJSON.Receivers[0].Name)
	require.NoError(t, s.StopService("alertmanager"))
}
