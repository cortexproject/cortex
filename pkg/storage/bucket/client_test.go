package bucket

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

const (
	configWithS3Backend = `
backend: s3
s3:
  endpoint:          localhost
  bucket_name:       test
  access_key_id:     xxx
  secret_access_key: yyy
  insecure:          true
`

	configWithGCSBackend = `
backend: gcs
gcs:
  bucket_name:     test
  service_account: |-
    {
      "type": "service_account",
      "project_id": "id",
      "private_key_id": "id",
      "private_key": "-----BEGIN PRIVATE KEY-----\nSOMETHING\n-----END PRIVATE KEY-----\n",
      "client_email": "test@test.com",
      "client_id": "12345",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test.com"
    }
`

	configWithUnknownBackend = `
backend: unknown
`
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config      string
		expectedErr error
	}{
		"should create an S3 bucket": {
			config:      configWithS3Backend,
			expectedErr: nil,
		},
		"should create a GCS bucket": {
			config:      configWithGCSBackend,
			expectedErr: nil,
		},
		"should return error on unknown backend": {
			config:      configWithUnknownBackend,
			expectedErr: ErrUnsupportedStorageBackend,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Load config
			cfg := Config{}
			flagext.DefaultValues(&cfg)

			err := yaml.Unmarshal([]byte(testData.config), &cfg)
			require.NoError(t, err)

			// Instance a new bucket client from the config
			bucketClient, err := NewClient(context.Background(), cfg, "test", util_log.Logger, nil)
			require.Equal(t, testData.expectedErr, err)

			if testData.expectedErr == nil {
				require.NotNil(t, bucketClient)
				bucketClient.Close()
			} else {
				assert.Equal(t, nil, bucketClient)
			}
		})
	}
}

func Test_Thanos_Metrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	ctx := context.Background()

	m := &ClientMock{}
	m.On("Get", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("error"))
	bkt := bucketWithMetrics(m, "", reg)
	_, err := bkt.Get(ctx, "something")
	require.Error(t, err)

	// Should report the metrics with `thanos_` prefix
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP thanos_objstore_bucket_operation_failures_total Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.
		# TYPE thanos_objstore_bucket_operation_failures_total counter
		thanos_objstore_bucket_operation_failures_total{bucket="",component="",operation="attributes"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",component="",operation="delete"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",component="",operation="exists"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",component="",operation="get"} 1
		thanos_objstore_bucket_operation_failures_total{bucket="",component="",operation="get_range"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",component="",operation="iter"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",component="",operation="upload"} 0
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
		# TYPE thanos_objstore_bucket_operations_total counter
		thanos_objstore_bucket_operations_total{bucket="",component="",operation="attributes"} 0
		thanos_objstore_bucket_operations_total{bucket="",component="",operation="delete"} 0
		thanos_objstore_bucket_operations_total{bucket="",component="",operation="exists"} 0
		thanos_objstore_bucket_operations_total{bucket="",component="",operation="get"} 1
		thanos_objstore_bucket_operations_total{bucket="",component="",operation="get_range"} 0
		thanos_objstore_bucket_operations_total{bucket="",component="",operation="iter"} 0
		thanos_objstore_bucket_operations_total{bucket="",component="",operation="upload"} 0
	`),
		"thanos_objstore_bucket_operations_total",
		"thanos_objstore_bucket_operation_failures_total",
	))
}

func TestClientMock_MockGet(t *testing.T) {
	expected := "body"

	m := ClientMock{}
	m.MockGet("test", expected, nil)

	// Run many goroutines all requesting the same mocked object and
	// ensure there's no race.
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			reader, err := m.Get(context.Background(), "test")
			require.NoError(t, err)
			actual, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, []byte(expected), actual)

			require.NoError(t, reader.Close())
		}()
	}

	wg.Wait()
}
