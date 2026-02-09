package overrides

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		initConfig func(*runtimeconfig.Config)
		expected   error
	}{
		"default config should pass": {
			initConfig: func(cfg *runtimeconfig.Config) {
				// Set default values for bucket config
				flagext.DefaultValues(&cfg.StorageConfig)
			},
			expected: nil,
		},
		"s3 config should pass": {
			initConfig: func(cfg *runtimeconfig.Config) {
				cfg.StorageConfig = bucket.Config{
					Backend: bucket.S3,
					S3: s3.Config{
						AccessKeyID:     "test-access-key",
						SecretAccessKey: flagext.Secret{Value: "test-secret-key"},
						BucketName:      "test-bucket",
						Endpoint:        "localhost:9000",
						Insecure:        true,
					},
				}
				// Set default values before validation
				flagext.DefaultValues(&cfg.StorageConfig.S3)
			},
			expected: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			cfg := runtimeconfig.Config{}

			testData.initConfig(&cfg)

			if testData.expected == nil {
				assert.NoError(t, cfg.StorageConfig.Validate())
			} else {
				assert.ErrorIs(t, cfg.StorageConfig.Validate(), testData.expected)
			}
		})
	}
}

func TestConfig_RegisterFlags(t *testing.T) {
	cfg := runtimeconfig.Config{}

	// Test that flags are registered without panicking
	require.NotPanics(t, func() {
		flagSet := flag.NewFlagSet("test", flag.PanicOnError)
		cfg.RegisterFlags(flagSet)
	})
}

func TestNew(t *testing.T) {
	tests := map[string]struct {
		cfg         runtimeconfig.Config
		expectError bool
	}{
		"valid config should create API": {
			cfg: func() runtimeconfig.Config {
				cfg := runtimeconfig.Config{
					StorageConfig: bucket.Config{
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
				// Set default values before validation
				flagext.DefaultValues(&cfg.StorageConfig.S3)
				return cfg
			}(),
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
			}
		})
	}
}

func TestOverridesModuleServiceInterface(t *testing.T) {
	// Create the API instance with proper configuration
	cfg := runtimeconfig.Config{
		StorageConfig: bucket.Config{
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
	// Set default values before validation
	flagext.DefaultValues(&cfg.StorageConfig.S3)
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
		requestBody      any
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
			expectedStatus: http.StatusNotFound,
			setupMock: func(mock *bucket.ClientMock) {
				// Mock that no overrides exist by passing empty content
				mock.MockGet("runtime.yaml", "overrides:\n", nil)
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
				var response map[string]any
				err := json.Unmarshal(recorder.Body.Bytes(), &response)
				require.NoError(t, err)
				assert.Equal(t, float64(5000), response["ingestion_rate"])
				assert.Equal(t, float64(100000), response["max_global_series_per_user"])
			},
		},
		{
			name:           "GET overrides - valid tenant ID, user does not exist",
			method:         "GET",
			path:           "/api/v1/user-overrides",
			tenantID:       "nonexistent_user",
			expectedStatus: http.StatusBadRequest,
			setupMock: func(mock *bucket.ClientMock) {
				// Mock runtime config with different user
				overridesData := `overrides:
  other_user:
    ingestion_rate: 5000`
				mock.MockGet("runtime.yaml", overridesData, nil)
			},
		},
		{
			name:           "POST overrides - no tenant ID",
			method:         "POST",
			path:           "/api/v1/user-overrides",
			tenantID:       "",
			requestBody:    map[string]any{"ingestion_rate": 5000},
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "POST overrides - valid tenant ID, valid overrides",
			method:         "POST",
			path:           "/api/v1/user-overrides",
			tenantID:       "user789",
			requestBody:    map[string]any{"ingestion_rate": 5000, "ruler_max_rules_per_rule_group": 10},
			expectedStatus: http.StatusOK,
			setupMock: func(mock *bucket.ClientMock) {
				// Mock runtime config with allowed limits
				runtimeConfig := `overrides:
  user789:
    ingestion_rate: 5000
    ruler_max_rules_per_rule_group: 10
api_allowed_limits:
  - ingestion_rate
  - ruler_max_rules_per_rule_group
  - max_global_series_per_user
  - max_global_series_per_metric
  - ingestion_burst_size
  - ruler_max_rule_groups_per_tenant`
				// Mock both reads: one for getAllowedLimitsFromBucket, one for setOverridesToBucket
				mock.MockGet("runtime.yaml", runtimeConfig, nil)
				mock.MockGet("runtime.yaml", runtimeConfig, nil)
				mock.MockUpload("runtime.yaml", nil)
			},
		},
		{
			name:           "POST overrides - invalid limit name",
			method:         "POST",
			path:           "/api/v1/user-overrides",
			tenantID:       "user999",
			requestBody:    map[string]any{"invalid_limit": 5000},
			expectedStatus: http.StatusBadRequest,
			setupMock: func(mock *bucket.ClientMock) {
				// Mock runtime config with allowed limits (invalid_limit not included)
				runtimeConfig := `api_allowed_limits:
  - ingestion_rate
  - ruler_max_rules_per_rule_group
  - max_global_series_per_user
  - max_global_series_per_metric
  - ingestion_burst_size
  - ruler_max_rule_groups_per_tenant`
				mock.MockGet("runtime.yaml", runtimeConfig, nil)
			},
		},

		{
			name:           "POST overrides - invalid JSON",
			method:         "POST",
			path:           "/api/v1/user-overrides",
			tenantID:       "user999",
			requestBody:    "invalid json",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "POST overrides - merge with existing overrides",
			method:         "POST",
			path:           "/api/v1/user-overrides",
			tenantID:       "user888",
			requestBody:    map[string]any{"ingestion_rate": 8000}, // Only update ingestion_rate
			expectedStatus: http.StatusOK,
			setupMock: func(m *bucket.ClientMock) {
				// Mock runtime config with existing overrides for user888
				initialConfig := `overrides:
  user888:
    ingestion_rate: 5000
    max_global_series_per_user: 100000
    ruler_max_rules_per_rule_group: 20
api_allowed_limits:
  - ingestion_rate
  - max_global_series_per_user
  - ruler_max_rules_per_rule_group
  - ingestion_burst_size`
				// First read for getAllowedLimitsFromBucket
				m.MockGet("runtime.yaml", initialConfig, nil)
				// Second read for setOverridesToBucket to get existing overrides
				m.MockGet("runtime.yaml", initialConfig, nil)

				// Mock upload and validate the merged content
				m.On("Upload", mock.Anything, "runtime.yaml", mock.Anything, mock.Anything).
					Return(nil).
					Run(func(args mock.Arguments) {
						// Read the uploaded content
						reader := args.Get(2).(io.Reader)
						content, err := io.ReadAll(reader)
						if err != nil {
							panic(fmt.Sprintf("failed to read uploaded content: %v", err))
						}

						// Verify that the uploaded content has merged values
						var config runtimeconfig.RuntimeConfigValues
						if err := yaml.Unmarshal(content, &config); err != nil {
							panic(fmt.Sprintf("failed to unmarshal uploaded config: %v", err))
						}

						// Check that all three fields are present
						if config.TenantLimits == nil || config.TenantLimits["user888"] == nil {
							panic("tenant limits for user888 not found")
						}

						limits := config.TenantLimits["user888"]

						// Verify ingestion_rate was updated
						if limits.IngestionRate != 8000 {
							panic(fmt.Sprintf("expected ingestion_rate to be 8000, got %f", limits.IngestionRate))
						}

						// Verify max_global_series_per_user was preserved
						if limits.MaxGlobalSeriesPerUser != 100000 {
							panic(fmt.Sprintf("expected max_global_series_per_user to be preserved at 100000, got %d", limits.MaxGlobalSeriesPerUser))
						}

						// Verify ruler_max_rules_per_rule_group was preserved
						if limits.RulerMaxRulesPerRuleGroup != 20 {
							panic(fmt.Sprintf("expected ruler_max_rules_per_rule_group to be preserved at 20, got %d", limits.RulerMaxRulesPerRuleGroup))
						}
					})
			},
		},
		{
			name:           "POST overrides - exceeding hard limit from runtime config",
			method:         "POST",
			path:           "/api/v1/user-overrides",
			tenantID:       "user999",
			requestBody:    map[string]any{"ingestion_rate": 1500000}, // Exceeds hard limit of 1000000
			expectedStatus: http.StatusBadRequest,
			setupMock: func(mock *bucket.ClientMock) {
				// Mock runtime config with per-user hard limits and allowed limits
				runtimeConfig := `overrides:
  user999:
    ingestion_rate: 1000
hard_overrides:
  user999:
    ingestion_rate: 1000000
    max_global_series_per_user: 5000000
api_allowed_limits:
  - ingestion_rate
  - max_global_series_per_user`
				// Mock all reads: one for getAllowedLimitsFromBucket, one for validateHardLimits, one for setOverridesToBucket
				mock.MockGet("runtime.yaml", runtimeConfig, nil)
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
			cfg := runtimeconfig.Config{
				StorageConfig: bucket.Config{
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
			// Set default values before validation
			flagext.DefaultValues(&cfg.StorageConfig.S3)
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
			case "POST":
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
			cfg := runtimeconfig.Config{
				StorageConfig: bucket.Config{
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
			// Set default values before validation
			flagext.DefaultValues(&cfg.StorageConfig.S3)
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
			name:     "GET overrides - bucket error returns internal server error",
			method:   "GET",
			tenantID: "user123",
			setupMock: func(mock *bucket.ClientMock) {
				mock.MockGet("runtime.yaml", "", fmt.Errorf("bucket error"))
			},
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:     "POST overrides - bucket upload error",
			method:   "POST",
			tenantID: "user456",
			setupMock: func(mock *bucket.ClientMock) {
				// Mock runtime config with allowed limits
				runtimeConfig := `overrides:
  user456:
    ingestion_rate: 1000
api_allowed_limits:
  - ingestion_rate
  - max_global_series_per_user`
				// First read succeeds (for allowed limits), then upload fails
				mock.MockGet("runtime.yaml", runtimeConfig, nil)
				mock.MockGet("runtime.yaml", runtimeConfig, nil)
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
			cfg := runtimeconfig.Config{
				StorageConfig: bucket.Config{
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
			// Set default values before validation
			flagext.DefaultValues(&cfg.StorageConfig.S3)
			api, err := New(cfg, log.Logger, prometheus.DefaultRegisterer)
			require.NoError(t, err)
			require.NotNil(t, api)

			// Manually set the bucket client and runtime config path for testing
			api.bucketClient = mockBucket
			api.runtimeConfigPath = "runtime.yaml"

			// Create the request
			var req *http.Request
			if tt.method == "POST" {
				requestBody := map[string]any{"ingestion_rate": 5000}
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
			case "POST":
				api.SetOverrides(recorder, req)
			case "DELETE":
				api.DeleteOverrides(recorder, req)
			}

			// Assert status code
			assert.Equal(t, tt.expectedStatus, recorder.Code)
		})
	}
}
