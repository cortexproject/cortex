//go:build integration_overrides

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
		"api_allowed_limits": []string{
			"ingestion_rate",
			"max_global_series_per_user",
			"max_global_series_per_metric",
			"ingestion_burst_size",
			"ruler_max_rules_per_rule_group",
			"ruler_max_rule_groups_per_tenant",
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

		"-runtime-config.file":                 "runtime.yaml",
		"-runtime-config.backend":              "s3",
		"-runtime-config.s3.access-key-id":     e2edb.MinioAccessKey,
		"-runtime-config.s3.secret-access-key": e2edb.MinioSecretKey,
		"-runtime-config.s3.bucket-name":       "cortex",
		"-runtime-config.s3.endpoint":          minio.NetworkHTTPEndpoint(),
		"-runtime-config.s3.insecure":          "true",
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

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("POST overrides for new user", func(t *testing.T) {
		newOverrides := map[string]interface{}{
			"ingestion_rate":       6000,
			"ingestion_burst_size": 7000,
		}
		requestBody, err := json.Marshal(newOverrides)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader(requestBody))
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

	t.Run("POST overrides with invalid limit", func(t *testing.T) {
		invalidOverrides := map[string]interface{}{
			"invalid_limit": 5000,
		}
		requestBody, err := json.Marshal(invalidOverrides)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader(requestBody))
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user4")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("POST overrides with invalid JSON", func(t *testing.T) {
		req, err := http.NewRequest("POST", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader([]byte("invalid json")))
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

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	require.NoError(t, s.Stop(cortexSvc))
}

func TestOverridesAPIHardLimits(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	minio := e2edb.NewMinio(9001, "cortex")
	require.NoError(t, s.StartAndWaitReady(minio))

	// Runtime config with hard limits
	runtimeConfig := map[string]interface{}{
		"overrides": map[string]interface{}{},
		"hard_overrides": map[string]interface{}{
			"user1": map[string]interface{}{
				"ingestion_rate":             10000,
				"max_global_series_per_user": 50000,
			},
		},
		"api_allowed_limits": []string{
			"ingestion_rate",
			"max_global_series_per_user",
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
	}, "overrides-test-hard-limits", nil)
	require.NoError(t, err)

	require.NoError(t, s3Client.Upload(context.Background(), "runtime.yaml", bytes.NewReader(runtimeConfigData)))

	flags := map[string]string{
		"-target": "overrides",

		"-runtime-config.file":                 "runtime.yaml",
		"-runtime-config.backend":              "s3",
		"-runtime-config.s3.access-key-id":     e2edb.MinioAccessKey,
		"-runtime-config.s3.secret-access-key": e2edb.MinioSecretKey,
		"-runtime-config.s3.bucket-name":       "cortex",
		"-runtime-config.s3.endpoint":          minio.NetworkHTTPEndpoint(),
		"-runtime-config.s3.insecure":          "true",
	}

	cortexSvc := e2ecortex.NewSingleBinary("cortex-overrides-hard-limits", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortexSvc))

	t.Run("POST overrides within hard limits", func(t *testing.T) {
		overrides := map[string]interface{}{
			"ingestion_rate":             5000,  // Within hard limit of 10000
			"max_global_series_per_user": 25000, // Within hard limit of 50000
		}
		requestBody, err := json.Marshal(overrides)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader(requestBody))
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user1")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("POST overrides exceeding hard limits", func(t *testing.T) {
		overrides := map[string]interface{}{
			"ingestion_rate": 15000, // Exceeds hard limit of 10000
		}
		requestBody, err := json.Marshal(overrides)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader(requestBody))
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user1")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("POST overrides for user without hard limits", func(t *testing.T) {
		overrides := map[string]interface{}{
			"ingestion_rate": 20000, // No hard limits for user2
		}
		requestBody, err := json.Marshal(overrides)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader(requestBody))
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user2")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	require.NoError(t, s.Stop(cortexSvc))
}

func TestOverridesAPIWithS3Error(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure Cortex to use S3 but don't start any S3 backend
	// This will cause all S3 operations to fail
	flags := map[string]string{
		"-target": "overrides",

		"-runtime-config.file":                 "runtime.yaml",
		"-runtime-config.backend":              "s3",
		"-runtime-config.s3.access-key-id":     e2edb.MinioAccessKey,
		"-runtime-config.s3.secret-access-key": e2edb.MinioSecretKey,
		"-runtime-config.s3.bucket-name":       "cortex",
		"-runtime-config.s3.endpoint":          "localhost:9002",
		"-runtime-config.s3.insecure":          "true",
	}

	cortexSvc := e2ecortex.NewSingleBinary("cortex-overrides-s3-error", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortexSvc))

	t.Run("GET overrides when S3 is unavailable", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user1")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("POST overrides when S3 is unavailable", func(t *testing.T) {
		newOverrides := map[string]interface{}{
			"ingestion_rate": 6000,
		}
		requestBody, err := json.Marshal(newOverrides)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", bytes.NewReader(requestBody))
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user1")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("DELETE overrides when S3 is unavailable", func(t *testing.T) {
		req, err := http.NewRequest("DELETE", "http://"+cortexSvc.HTTPEndpoint()+"/api/v1/user-overrides", nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user1")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})

	require.NoError(t, s.Stop(cortexSvc))
}

func TestOverridesAPITenantExtraction(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	minio := e2edb.NewMinio(9010, "cortex")
	require.NoError(t, s.StartAndWaitReady(minio))

	// Upload an empty runtime config file to S3
	runtimeConfig := map[string]interface{}{
		"overrides": map[string]interface{}{},
	}
	runtimeConfigData, err := yaml.Marshal(runtimeConfig)
	require.NoError(t, err)

	s3Client, err := s3.NewBucketWithConfig(nil, s3.Config{
		Endpoint:  minio.HTTPEndpoint(),
		Insecure:  true,
		Bucket:    "cortex",
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
	}, "overrides-test-tenant", nil)
	require.NoError(t, err)

	require.NoError(t, s3Client.Upload(context.Background(), "runtime.yaml", bytes.NewReader(runtimeConfigData)))

	flags := map[string]string{
		"-target": "overrides",

		"-runtime-config.file":                 "runtime.yaml",
		"-runtime-config.backend":              "s3",
		"-runtime-config.s3.access-key-id":     e2edb.MinioAccessKey,
		"-runtime-config.s3.secret-access-key": e2edb.MinioSecretKey,
		"-runtime-config.s3.bucket-name":       "cortex",
		"-runtime-config.s3.endpoint":          minio.NetworkHTTPEndpoint(),
		"-runtime-config.s3.insecure":          "true",
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
