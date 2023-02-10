package s3

import (
	"encoding/base64"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	bucket_http "github.com/cortexproject/cortex/pkg/storage/bucket/http"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// defaultConfig should match the default flag values defined in RegisterFlagsWithPrefix.
var defaultConfig = Config{
	SignatureVersion: SignatureVersionV4,
	BucketLookupType: BucketAutoLookup,
	OperationRetries: 1,
	RetryMinBackoff:  10 * time.Second,
	RetryMaxBackoff:  time.Minute,
	HTTP: HTTPConfig{
		Config: bucket_http.Config{
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: 2 * time.Minute,
			InsecureSkipVerify:    false,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
			MaxConnsPerHost:       0,
		},
	},
}

func TestConfig(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config         string
		expectedConfig Config
		expectedErr    error
	}{
		"default config": {
			config:         "",
			expectedConfig: defaultConfig,
			expectedErr:    nil,
		},
		"custom config": {
			config: `
endpoint: test-endpoint
region: test-region
bucket_name: test-bucket-name
secret_access_key: test-secret-access-key
access_key_id: test-access-key-id
insecure: true
signature_version: test-signature-version
bucket_lookup_type: virtual-hosted
operation_retries: 10
retry_min_backoff: 5s
retry_max_backoff: 2m
sse:
  type: test-type
  kms_key_id: test-kms-key-id
  kms_encryption_context: test-kms-encryption-context
http:
  idle_conn_timeout: 2s
  response_header_timeout: 3s
  insecure_skip_verify: true
  tls_handshake_timeout: 4s
  expect_continue_timeout: 5s
  max_idle_connections: 6
  max_idle_connections_per_host: 7
  max_connections_per_host: 8
`,
			expectedConfig: Config{
				Endpoint:         "test-endpoint",
				Region:           "test-region",
				BucketName:       "test-bucket-name",
				SecretAccessKey:  flagext.Secret{Value: "test-secret-access-key"},
				AccessKeyID:      "test-access-key-id",
				Insecure:         true,
				SignatureVersion: "test-signature-version",
				BucketLookupType: BucketVirtualHostLookup,
				OperationRetries: 10,
				RetryMinBackoff:  5 * time.Second,
				RetryMaxBackoff:  2 * time.Minute,
				SSE: SSEConfig{
					Type:                 "test-type",
					KMSKeyID:             "test-kms-key-id",
					KMSEncryptionContext: "test-kms-encryption-context",
				},
				HTTP: HTTPConfig{
					Config: bucket_http.Config{
						IdleConnTimeout:       2 * time.Second,
						ResponseHeaderTimeout: 3 * time.Second,
						InsecureSkipVerify:    true,
						TLSHandshakeTimeout:   4 * time.Second,
						ExpectContinueTimeout: 5 * time.Second,
						MaxIdleConns:          6,
						MaxIdleConnsPerHost:   7,
						MaxConnsPerHost:       8,
					},
				},
			},
			expectedErr: nil,
		},
		"invalid type": {
			config:         `insecure: foo`,
			expectedConfig: defaultConfig,
			expectedErr:    &yaml.TypeError{Errors: []string{"line 1: cannot unmarshal !!str `foo` into bool"}},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)

			err := yaml.Unmarshal([]byte(testData.config), &cfg)
			require.Equal(t, testData.expectedErr, err)
			require.Equal(t, testData.expectedConfig, cfg)
		})
	}
}

func TestSSEConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup    func() *SSEConfig
		expected error
	}{
		"should pass with default config": {
			setup: func() *SSEConfig {
				cfg := &SSEConfig{}
				flagext.DefaultValues(cfg)

				return cfg
			},
		},
		"should fail on invalid SSE type": {
			setup: func() *SSEConfig {
				return &SSEConfig{
					Type: "unknown",
				}
			},
			expected: errUnsupportedSSEType,
		},
		"should fail on invalid SSE KMS encryption context": {
			setup: func() *SSEConfig {
				return &SSEConfig{
					Type:                 SSEKMS,
					KMSEncryptionContext: "!{}!",
				}
			},
			expected: errInvalidSSEContext,
		},
		"should pass on valid SSE KMS encryption context": {
			setup: func() *SSEConfig {
				return &SSEConfig{
					Type:                 SSEKMS,
					KMSEncryptionContext: `{"department": "10103.0"}`,
				}
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.setup().Validate())
		})
	}
}

func TestSSEConfig_BuildMinioConfig(t *testing.T) {
	tests := map[string]struct {
		cfg             *SSEConfig
		expectedType    string
		expectedKeyID   string
		expectedContext string
	}{
		"SSE KMS without encryption context": {
			cfg: &SSEConfig{
				Type:     SSEKMS,
				KMSKeyID: "test-key",
			},
			expectedType:    "aws:kms",
			expectedKeyID:   "test-key",
			expectedContext: "",
		},
		"SSE KMS with encryption context": {
			cfg: &SSEConfig{
				Type:                 SSEKMS,
				KMSKeyID:             "test-key",
				KMSEncryptionContext: "{\"department\":\"10103.0\"}",
			},
			expectedType:    "aws:kms",
			expectedKeyID:   "test-key",
			expectedContext: "{\"department\":\"10103.0\"}",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			sse, err := testData.cfg.BuildMinioConfig()
			require.NoError(t, err)

			headers := http.Header{}
			sse.Marshal(headers)

			assert.Equal(t, testData.expectedType, headers.Get("x-amz-server-side-encryption"))
			assert.Equal(t, testData.expectedKeyID, headers.Get("x-amz-server-side-encryption-aws-kms-key-id"))
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(testData.expectedContext)), headers.Get("x-amz-server-side-encryption-context"))
		})
	}
}

func TestParseKMSEncryptionContext(t *testing.T) {
	actual, err := parseKMSEncryptionContext("")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string(nil), actual)

	expected := map[string]string{
		"department": "10103.0",
	}
	actual, err = parseKMSEncryptionContext(`{"department": "10103.0"}`)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
