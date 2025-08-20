//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/s3"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestOverridesAPIWithRunningCortex(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	minio := e2edb.NewMinio(9000, "cortex")
	require.NoError(t, s.StartAndWaitReady(minio))

	runtimeConfig := map[string]interface{}{
		"overrides": map[string]interface{}{
			"user1": map[string]interface{}{
				"ingestion_rate": 5000,
			},
		},
	}
	runtimeConfigData, err := yaml.Marshal(runtimeConfig)
	require.NoError(t, err)

	s3Client, err := s3.NewBucketWithConfig(nil, s3.Config{
		Endpoint:  minio.HTTPEndpoint(),
		Insecure:  true,
		Bucket:    "cortex",
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
	}, "overrides-test", nil)
	require.NoError(t, err)

	require.NoError(t, s3Client.Upload(context.Background(), "runtime.yaml", bytes.NewReader(runtimeConfigData)))

	flags := map[string]string{
		"-target": "overrides",

		"-overrides.runtime-config-file":  "runtime.yaml",
		"-overrides.backend":              "s3",
		"-overrides.s3.access-key-id":     e2edb.MinioAccessKey,
		"-overrides.s3.secret-access-key": e2edb.MinioSecretKey,
		"-overrides.s3.bucket-name":       "cortex",
		"-overrides.s3.endpoint":          minio.NetworkHTTPEndpoint(),
		"-overrides.s3.insecure":          "true",
	}

	cortexSvc := e2ecortex.NewSingleBinary("cortex-overrides", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortexSvc))

	t.Run("GET overrides for existing user", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user1")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var overrides map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&overrides)
		require.NoError(t, err)

		assert.Equal(t, float64(5000), overrides["ingestion_rate"])
	})

	t.Run("GET overrides for non-existing user", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user2")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var overrides map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&overrides)
		require.NoError(t, err)

		assert.Empty(t, overrides)
	})

	t.Run("PUT overrides for new user", func(t *testing.T) {
		newOverrides := map[string]interface{}{
			"ingestion_rate":       6000,
			"ingestion_burst_size": 7000,
		}
		requestBody, err := json.Marshal(newOverrides)
		require.NoError(t, err)

		req, err := http.NewRequest("PUT", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader(requestBody))
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user3")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		req, err = http.NewRequest("GET", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user3")

		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var savedOverrides map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&savedOverrides)
		require.NoError(t, err)

		assert.Equal(t, float64(6000), savedOverrides["ingestion_rate"])
		assert.Equal(t, float64(7000), savedOverrides["ingestion_burst_size"])
	})

	t.Run("PUT overrides with invalid limit", func(t *testing.T) {
		invalidOverrides := map[string]interface{}{
			"invalid_limit": 5000,
		}
		requestBody, err := json.Marshal(invalidOverrides)
		require.NoError(t, err)

		req, err := http.NewRequest("PUT", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader(requestBody))
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user4")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("PUT overrides with invalid JSON", func(t *testing.T) {
		req, err := http.NewRequest("PUT", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader([]byte("invalid json")))
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user5")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("DELETE overrides", func(t *testing.T) {
		req, err := http.NewRequest("DELETE", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user1")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		req, err = http.NewRequest("GET", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user1")

		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var overrides map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&overrides)
		require.NoError(t, err)

		assert.Empty(t, overrides)
	})

	require.NoError(t, s.Stop(cortexSvc))
}

func TestOverridesAPITenantExtraction(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul-tenant")
	require.NoError(t, s.StartAndWaitReady(consul))

	minio := e2edb.NewMinio(9010, "cortex")
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := map[string]string{
		"-target": "overrides",

		"-overrides.runtime-config-file":  "runtime.yaml",
		"-overrides.backend":              "s3",
		"-overrides.s3.access-key-id":     e2edb.MinioAccessKey,
		"-overrides.s3.secret-access-key": e2edb.MinioSecretKey,
		"-overrides.s3.bucket-name":       "cortex",
		"-overrides.s3.endpoint":          minio.NetworkHTTPEndpoint(),
		"-overrides.s3.insecure":          "true",
		"-ring.store":                     "consul",
		"-consul.hostname":                consul.NetworkHTTPEndpoint(),
	}

	cortexSvc := e2ecortex.NewSingleBinary("cortex-overrides-tenant", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortexSvc))

	t.Run("no tenant header", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("empty tenant header", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	require.NoError(t, s.Stop(cortexSvc))
}

func TestOverridesAPIFilesystemBackendRejected(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	t.Run("filesystem backend should be rejected", func(t *testing.T) {
		flags := map[string]string{
			"-target":                        "overrides",
			"-overrides.runtime-config-file": "runtime.yaml",
			"-overrides.backend":             "filesystem",
			"-ring.store":                    "consul",
			"-consul.hostname":               "localhost:8500",
		}

		cortexSvc := e2ecortex.NewSingleBinary("cortex-overrides-filesystem", flags, "")

		err = s.StartAndWaitReady(cortexSvc)
		if err == nil {
			t.Error("Expected Cortex to fail to start with filesystem backend, but it started successfully")
			require.NoError(t, s.Stop(cortexSvc))
		} else {
			t.Logf("Expected failure with filesystem backend: %v", err)
		}
	})

	t.Run("no backend specified should be rejected", func(t *testing.T) {
		flags := map[string]string{
			"-target":                        "overrides",
			"-overrides.runtime-config-file": "runtime.yaml",
			"-ring.store":                    "consul",
			"-consul.hostname":               "localhost:8500",
		}

		cortexSvc := e2ecortex.NewSingleBinary("cortex-overrides-no-backend", flags, "")

		err = s.StartAndWaitReady(cortexSvc)
		if err == nil {
			t.Error("Expected Cortex to fail to start with no backend specified, but it started successfully")
			require.NoError(t, s.Stop(cortexSvc))
		} else {
			t.Logf("Expected failure with no backend specified: %v", err)
		}
	})
}
