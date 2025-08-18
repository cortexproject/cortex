package overrides

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		initConfig func(*Config)
		expected   error
	}{
		"default config should pass": {
			initConfig: func(_ *Config) {},
			expected:   nil,
		},
		"disabled config should pass": {
			initConfig: func(cfg *Config) {

			},
			expected: nil,
		},
		"enabled config should pass": {
			initConfig: func(cfg *Config) {
				cfg.Config = bucket.Config{
					Backend: bucket.S3,
					S3: s3.Config{
						AccessKeyID:     "test-access-key",
						SecretAccessKey: flagext.Secret{Value: "test-secret-key"},
						BucketName:      "test-bucket",
						Endpoint:        "localhost:9000",
						Insecure:        true,
					},
				}
			},
			expected: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			cfg := Config{}

			testData.initConfig(&cfg)

			if testData.expected == nil {
				assert.NoError(t, cfg.Validate())
			} else {
				assert.ErrorIs(t, cfg.Validate(), testData.expected)
			}
		})
	}
}

func TestConfig_RegisterFlags(t *testing.T) {
	cfg := Config{}

	// Test that flags are registered without panicking
	require.NotPanics(t, func() {
		flagSet := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(flagSet)
	})
}

func TestNew(t *testing.T) {
	tests := map[string]struct {
		cfg         Config
		expectError bool
	}{
		"valid config should create API": {
			cfg: Config{
				Config: bucket.Config{
					Backend: bucket.S3,
					S3: s3.Config{
						AccessKeyID:     "test-access-key",
						SecretAccessKey: flagext.Secret{Value: "test-secret-key"},
						BucketName:      "test-bucket",
						Endpoint:        "localhost:9000",
						Insecure:        true,
					},
				},
			},
			expectError: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			api, err := New(testData.cfg, log.Logger, prometheus.DefaultRegisterer)

			if testData.expectError {
				assert.Error(t, err)
				assert.Nil(t, api)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, api)
				// Don't compare the entire config since defaults may modify it
				// Just verify the API was created successfully
			}
		})
	}
}

func TestOverridesModuleServiceInterface(t *testing.T) {
	// Create the API instance with proper configuration
	cfg := Config{
		Config: bucket.Config{
			Backend: bucket.S3,
			S3: s3.Config{
				AccessKeyID:     "test-access-key",
				SecretAccessKey: flagext.Secret{Value: "test-secret-key"},
				BucketName:      "test-bucket",
				Endpoint:        "localhost:9000",
				Insecure:        true,
			},
		},
	}
	api, err := New(cfg, log.Logger, prometheus.DefaultRegisterer)
	require.NoError(t, err)
	require.NotNil(t, api)

	// Verify it implements the Service interface
	require.Implements(t, (*services.Service)(nil), api)

	// Verify initial state
	assert.Equal(t, services.New, api.State())

	// Verify the service has the expected methods
	// This is a basic check that the service was properly constructed
	assert.NotNil(t, api.Service)
}

// TestAPIEndpoints tests the actual HTTP API endpoints
func TestAPIEndpoints(t *testing.T) {
	tests := []struct {
		name             string
		method           string
		path             string
		tenantID         string
		requestBody      interface{}
		expectedStatus   int
		setupMock        func(*bucket.ClientMock)
		validateResponse func(*testing.T, *httptest.ResponseRecorder)
	}{
		{
			name:           "GET overrides - no tenant ID",
			method:         "GET",
			path:           "/api/v1/user-overrides",
			tenantID:       "",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "GET overrides - valid tenant ID, no overrides",
			method:         "GET",
			path:           "/api/v1/user-overrides",
			tenantID:       "user123",
			expectedStatus: http.StatusOK,
			setupMock: func(mock *bucket.ClientMock) {
				// Mock that no overrides exist by passing empty content
				mock.MockGet("runtime.yaml", "overrides:\n", nil)
			},
			validateResponse: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				var response map[string]interface{}
				err := json.Unmarshal(recorder.Body.Bytes(), &response)
				require.NoError(t, err)
				assert.Empty(t, response)
			},
		},
		{
			name:           "GET overrides - valid tenant ID, with overrides",
			method:         "GET",
			path:           "/api/v1/user-overrides",
			tenantID:       "user456",
			expectedStatus: http.StatusOK,
			setupMock: func(mock *bucket.ClientMock) {
				overridesData := `overrides:
  user456:
    ingestion_rate: 5000
    max_global_series_per_user: 100000`
				mock.MockGet("runtime.yaml", overridesData, nil)
			},
			validateResponse: func(t *testing.T, recorder *httptest.ResponseRecorder) {
				var response map[string]interface{}
				err := json.Unmarshal(recorder.Body.Bytes(), &response)
				require.NoError(t, err)
				assert.Equal(t, float64(5000), response["ingestion_rate"])
				assert.Equal(t, float64(100000), response["max_global_series_per_user"])
			},
		},
		{
			name:           "PUT overrides - no tenant ID",
			method:         "PUT",
			path:           "/api/v1/user-overrides",
			tenantID:       "",
			requestBody:    map[string]interface{}{"ingestion_rate": 5000},
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "PUT overrides - valid tenant ID, valid overrides",
			method:         "PUT",
			path:           "/api/v1/user-overrides",
			tenantID:       "user789",
			requestBody:    map[string]interface{}{"ingestion_rate": 5000, "ruler_max_rules_per_rule_group": 10},
			expectedStatus: http.StatusOK,
			setupMock: func(mock *bucket.ClientMock) {
				// First read succeeds, then upload succeeds
				mock.MockGet("runtime.yaml", "overrides:\n  user789:\n    ingestion_rate: 5000\n    ruler_max_rules_per_rule_group: 10\n", nil)
				mock.MockUpload("runtime.yaml", nil)
			},
		},
		{
			name:           "PUT overrides - invalid limit name",
			method:         "PUT",
			path:           "/api/v1/user-overrides",
			tenantID:       "user999",
			requestBody:    map[string]interface{}{"invalid_limit": 5000},
			expectedStatus: http.StatusBadRequest,
		},

		{
			name:           "PUT overrides - invalid JSON",
			method:         "PUT",
			path:           "/api/v1/user-overrides",
			tenantID:       "user999",
			requestBody:    "invalid json",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "PUT overrides - exceeding hard limit from runtime config",
			method:         "PUT",
			path:           "/api/v1/user-overrides",
			tenantID:       "user999",
			requestBody:    map[string]interface{}{"ingestion_rate": 1500000}, // Exceeds hard limit of 1000000
			expectedStatus: http.StatusBadRequest,
			setupMock: func(mock *bucket.ClientMock) {
				// Mock runtime config with per-user hard limits
				runtimeConfig := `overrides:
  user999:
    ingestion_rate: 1000
hard_overrides:
  user999:
    ingestion_rate: 1000000
    max_global_series_per_user: 5000000`
				// Mock both reads: one for validateHardLimits, one for setOverridesToBucket
				mock.MockGet("runtime.yaml", runtimeConfig, nil)
				mock.MockGet("runtime.yaml", runtimeConfig, nil)
				mock.MockUpload("runtime.yaml", nil)
			},
		},
		{
			name:           "DELETE overrides - no tenant ID",
			method:         "DELETE",
			path:           "/api/v1/user-overrides",
			tenantID:       "",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "DELETE overrides - valid tenant ID",
			method:         "DELETE",
			path:           "/api/v1/user-overrides",
			tenantID:       "user123",
			expectedStatus: http.StatusOK,
			setupMock: func(mock *bucket.ClientMock) {
				// First read succeeds, then upload succeeds
				mock.MockGet("runtime.yaml", "overrides:\n  user123:\n    ingestion_rate: 1000", nil)
				mock.MockUpload("runtime.yaml", nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock bucket client
			mockBucket := &bucket.ClientMock{}
			if tt.setupMock != nil {
				tt.setupMock(mockBucket)
			}

			// Create the API instance with proper configuration
			cfg := Config{
				Config: bucket.Config{
					Backend: bucket.S3,
					S3: s3.Config{
						AccessKeyID:     "test-access-key",
						SecretAccessKey: flagext.Secret{Value: "test-secret-key"},
						BucketName:      "test-bucket",
						Endpoint:        "localhost:9000",
						Insecure:        true,
					},
				},
			}
			api, err := New(cfg, log.Logger, prometheus.DefaultRegisterer)
			require.NoError(t, err)
			require.NotNil(t, api)

			// Manually set the bucket client and runtime config path for testing
			api.bucketClient = mockBucket
			api.runtimeConfigPath = "runtime.yaml"

			// Create the request
			var req *http.Request
			if tt.requestBody != nil {
				var body []byte
				if str, ok := tt.requestBody.(string); ok {
					body = []byte(str)
				} else {
					body, err = json.Marshal(tt.requestBody)
					require.NoError(t, err)
				}
				req = httptest.NewRequest(tt.method, tt.path, bytes.NewReader(body))
			} else {
				req = httptest.NewRequest(tt.method, tt.path, nil)
			}

			// Add tenant ID header if provided
			if tt.tenantID != "" {
				req.Header.Set("X-Scope-OrgID", tt.tenantID)
			}

			// Create response recorder
			recorder := httptest.NewRecorder()

			// Call the appropriate handler based on method
			switch tt.method {
			case "GET":
				api.GetOverrides(recorder, req)
			case "PUT":
				api.SetOverrides(recorder, req)
			case "DELETE":
				api.DeleteOverrides(recorder, req)
			default:
				t.Fatalf("Unsupported method: %s", tt.method)
			}

			// Assert status code
			assert.Equal(t, tt.expectedStatus, recorder.Code)

			// Validate response if validation function provided
			if tt.validateResponse != nil {
				tt.validateResponse(t, recorder)
			}
		})
	}
}

// TestAPITenantExtraction tests tenant ID extraction from various header formats
func TestAPITenantExtraction(t *testing.T) {
	tests := []struct {
		name           string
		headers        map[string]string
		expectedTenant string
		expectError    bool
		setupMock      func(*bucket.ClientMock)
	}{
		{
			name:           "X-Scope-OrgID header",
			headers:        map[string]string{"X-Scope-OrgID": "tenant1"},
			expectedTenant: "tenant1",
			expectError:    false,
			setupMock: func(mock *bucket.ClientMock) {
				// Mock successful get with empty overrides
				mock.MockGet("runtime.yaml", "overrides:\n", nil)
			},
		},
		{
			name:           "no tenant header",
			headers:        map[string]string{},
			expectedTenant: "",
			expectError:    true,
		},
		{
			name:           "empty tenant header",
			headers:        map[string]string{"X-Scope-OrgID": ""},
			expectedTenant: "",
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock bucket client
			mockBucket := &bucket.ClientMock{}
			if tt.setupMock != nil {
				tt.setupMock(mockBucket)
			}

			// Create the API instance with proper configuration
			cfg := Config{
				Config: bucket.Config{
					Backend: bucket.S3,
					S3: s3.Config{
						AccessKeyID:     "test-access-key",
						SecretAccessKey: flagext.Secret{Value: "test-secret-key"},
						BucketName:      "test-bucket",
						Endpoint:        "localhost:9000",
						Insecure:        true,
					},
				},
			}
			api, err := New(cfg, log.Logger, prometheus.DefaultRegisterer)
			require.NoError(t, err)
			require.NotNil(t, api)

			// Manually set the bucket client and runtime config path for testing
			api.bucketClient = mockBucket
			api.runtimeConfigPath = "runtime.yaml"

			// Create the request
			req := httptest.NewRequest("GET", "/api/v1/user-overrides", nil)
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			// Create response recorder
			recorder := httptest.NewRecorder()

			// Call the handler
			api.GetOverrides(recorder, req)

			// Assert based on expected behavior
			if tt.expectError {
				assert.Equal(t, http.StatusUnauthorized, recorder.Code)
			} else {
				assert.Equal(t, http.StatusOK, recorder.Code)
			}
		})
	}
}

// TestAPIBucketErrors tests how the API handles bucket operation errors
func TestAPIBucketErrors(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		tenantID       string
		setupMock      func(*bucket.ClientMock)
		expectedStatus int
	}{
		{
			name:     "GET overrides - bucket error treated as not found",
			method:   "GET",
			tenantID: "user123",
			setupMock: func(mock *bucket.ClientMock) {
				mock.MockGet("runtime.yaml", "", fmt.Errorf("bucket error"))
			},
			expectedStatus: http.StatusOK, // Current implementation treats errors as "not found"
		},
		{
			name:     "PUT overrides - bucket upload error",
			method:   "PUT",
			tenantID: "user456",
			setupMock: func(mock *bucket.ClientMock) {
				// First read succeeds, then upload fails
				mock.MockGet("runtime.yaml", "overrides:\n  user456:\n    ingestion_rate: 1000", nil)
				mock.MockUpload("runtime.yaml", fmt.Errorf("upload error"))
			},
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:     "DELETE overrides - bucket delete error",
			method:   "DELETE",
			tenantID: "user789",
			setupMock: func(mock *bucket.ClientMock) {
				// First read succeeds, then upload fails
				mock.MockGet("runtime.yaml", "overrides:\n  user789:\n    ingestion_rate: 1000", nil)
				mock.MockUpload("runtime.yaml", fmt.Errorf("upload error"))
			},
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock bucket client
			mockBucket := &bucket.ClientMock{}
			tt.setupMock(mockBucket)

			// Create the API instance with proper configuration
			cfg := Config{
				Config: bucket.Config{
					Backend: bucket.S3,
					S3: s3.Config{
						AccessKeyID:     "test-access-key",
						SecretAccessKey: flagext.Secret{Value: "test-secret-key"},
						BucketName:      "test-bucket",
						Endpoint:        "localhost:9000",
						Insecure:        true,
					},
				},
			}
			api, err := New(cfg, log.Logger, prometheus.DefaultRegisterer)
			require.NoError(t, err)
			require.NotNil(t, api)

			// Manually set the bucket client and runtime config path for testing
			api.bucketClient = mockBucket
			api.runtimeConfigPath = "runtime.yaml"

			// Create the request
			var req *http.Request
			if tt.method == "PUT" {
				requestBody := map[string]interface{}{"ingestion_rate": 5000}
				body, err := json.Marshal(requestBody)
				require.NoError(t, err)
				req = httptest.NewRequest(tt.method, "/api/v1/user-overrides", bytes.NewReader(body))
			} else {
				req = httptest.NewRequest(tt.method, "/api/v1/user-overrides", nil)
			}

			// Add tenant ID header
			req.Header.Set("X-Scope-OrgID", tt.tenantID)

			// Create response recorder
			recorder := httptest.NewRecorder()

			// Call the appropriate handler
			switch tt.method {
			case "GET":
				api.GetOverrides(recorder, req)
			case "PUT":
				api.SetOverrides(recorder, req)
			case "DELETE":
				api.DeleteOverrides(recorder, req)
			}

			// Assert status code
			assert.Equal(t, tt.expectedStatus, recorder.Code)
		})
	}
}
