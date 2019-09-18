package main

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestAlertmanager(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	alertmanagerDir := filepath.Join(s.SharedDir(), "alertmanager_configs")
	require.NoError(t, os.Mkdir(alertmanagerDir, os.ModePerm))

	require.NoError(t, ioutil.WriteFile(
		filepath.Join(alertmanagerDir, "user-1.yaml"),
		[]byte(cortexAlertmanagerUserConfigYaml),
		os.ModePerm),
	)

	alertmanager := e2ecortex.NewAlertmanager("alertmanager", AlertmanagerConfigs, "")
	require.NoError(t, s.StartAndWaitReady(alertmanager))
	require.NoError(t, alertmanager.WaitSumMetric("cortex_alertmanager_configs", 1))

	c, err := e2ecortex.NewClient("", "", alertmanager.Endpoint(80), "user-1")
	require.NoError(t, err)

	cfg, err := c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// Ensure the returned status config matches alertmanager_test_fixtures/user-1.yaml
	require.NotNil(t, cfg)
	require.Equal(t, "example_receiver", cfg.Route.Receiver)
	require.Len(t, cfg.Route.GroupByStr, 1)
	require.Equal(t, "example_groupby", cfg.Route.GroupByStr[0])
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "example_receiver", cfg.Receivers[0].Name)
}
