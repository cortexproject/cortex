// +build requires_docker

package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestConfigAPIEndpoint(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start Cortex in single binary mode, reading the config from file.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config.yaml", cortexConfigFile))
	flags := map[string]string{
		"-config.file": filepath.Join(e2e.ContainerSharedDir, cortexConfigFile),
	}

	cortex1 := e2ecortex.NewSingleBinary("cortex-1", flags, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex1))

	// Get config from /config API endpoint.
	res, err := e2e.GetRequest(fmt.Sprintf("http://%s/config", cortex1.Endpoint(9009)))
	require.NoError(t, err)

	defer runutil.ExhaustCloseWithErrCapture(&err, res.Body, "config API response")
	body, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Start again Cortex in single binary with the exported config
	// and ensure it starts (pass the readiness probe).
	require.NoError(t, writeFileToSharedDir(s, cortexConfigFile, body))
	cortex2 := e2ecortex.NewSingleBinary("cortex-2", flags, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex2))
}
