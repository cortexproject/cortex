package tsdb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore/gcs"
	yaml "gopkg.in/yaml.v2"
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

func TestNewBucketClient(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config       string
		expectedErr  error
		expectedType interface{}
	}{
		"should create an S3 bucket": {
			config:       configWithS3Backend,
			expectedErr:  nil,
			expectedType: &s3.Bucket{},
		},
		"should create a GCS bucket": {
			config:       configWithGCSBackend,
			expectedErr:  nil,
			expectedType: &gcs.Bucket{},
		},
		"should return error on unknown backend": {
			config:       configWithUnknownBackend,
			expectedErr:  errUnsupportedBackend,
			expectedType: nil,
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
			bucketClient, err := NewBucketClient(context.Background(), cfg, "test", util.Logger)
			require.Equal(t, testData.expectedErr, err)

			if testData.expectedErr == nil {
				require.NotNil(t, bucketClient)
				assert.IsType(t, testData.expectedType, bucketClient)

				bucketClient.Close()
			} else {
				assert.Equal(t, nil, bucketClient)
			}
		})
	}
}
