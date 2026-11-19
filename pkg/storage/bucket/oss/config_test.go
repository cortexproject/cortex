package oss

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func TestConfig(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config         string
		expectedConfig Config
		expectedErr    error
	}{
		"default config": {
			config:         "",
			expectedConfig: Config{},
			expectedErr:    nil,
		},
		"custom config": {
			config: `
endpoint: oss-cn-hangzhou.aliyuncs.com
bucket: test-bucket
access_key_id: test-access-key-id
access_key_secret: test-access-key-secret
`,
			expectedConfig: Config{
				Endpoint:        "oss-cn-hangzhou.aliyuncs.com",
				Bucket:          "test-bucket",
				AccessKeyID:     "test-access-key-id",
				AccessKeySecret: flagext.Secret{Value: "test-access-key-secret"},
			},
			expectedErr: nil,
		},
		"invalid type": {
			config:         `bucket: [foo]`,
			expectedConfig: Config{},
			expectedErr:    &yaml.TypeError{Errors: []string{"line 1: cannot unmarshal !!seq into string"}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)

			err := yaml.Unmarshal([]byte(testData.config), &cfg)
			require.Equal(t, testData.expectedErr, err)
			require.Equal(t, testData.expectedConfig, cfg)
		})
	}
}

func TestNewBucketClient(t *testing.T) {
	t.Parallel()

	_, err := NewBucketClient(Config{}, nil, "test", log.NewNopLogger())
	require.ErrorContains(t, err, "endpoint or bucket is not present")

	bkt, err := NewBucketClient(Config{
		Endpoint:        "oss-cn-hangzhou.aliyuncs.com",
		Bucket:          "test-bucket",
		AccessKeyID:     "id",
		AccessKeySecret: flagext.Secret{Value: "secret"},
	}, nil, "test", log.NewNopLogger())
	require.NoError(t, err)
	require.Equal(t, "test-bucket", bkt.Name())
}
