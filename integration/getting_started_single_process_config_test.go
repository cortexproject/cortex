package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestGettingStartedSingleProcessConfig(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config.yaml", cortexConfigFile))

	// Start Cortex in single binary mode, reading the config from file.
	flags := map[string]string{
		"-config.file": filepath.Join(e2e.ContainerSharedDir, cortexConfigFile),
	}

	cortex := e2ecortex.NewSingleBinary("cortex-1", flags, "", 9009)
	require.NoError(t, s.StartAndWaitReady(cortex))

	c, err := e2ecortex.NewClient(cortex.Endpoint(9009), cortex.Endpoint(9009), "user-1")
	require.NoError(t, err)

	// Push some series to Cortex.
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the series.
	result, err := c.Query("series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))
}
