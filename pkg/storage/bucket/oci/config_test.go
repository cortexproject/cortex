package oci

import (
	"testing"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var defaultConfig = Config{
	Provider:             "default",
	MaxRequestRetries:    3,
	RequestRetryInterval: 10,
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
provider: raw
bucket: test-bucket
compartment_ocid: ocid1.compartment.oc1..test
tenancy_ocid: ocid1.tenancy.oc1..test
user_ocid: ocid1.user.oc1..test
region: us-ashburn-1
fingerprint: aa:bb:cc:dd
privatekey: test-private-key
passphrase: test-passphrase
part_size: 134217728
max_request_retries: 5
request_retry_interval: 2
`,
			expectedConfig: Config{
				Provider:             "raw",
				Bucket:               "test-bucket",
				Compartment:          "ocid1.compartment.oc1..test",
				Tenancy:              "ocid1.tenancy.oc1..test",
				User:                 "ocid1.user.oc1..test",
				Region:               "us-ashburn-1",
				Fingerprint:          "aa:bb:cc:dd",
				PrivateKey:           flagext.Secret{Value: "test-private-key"},
				Passphrase:           flagext.Secret{Value: "test-passphrase"},
				PartSize:             134217728,
				MaxRequestRetries:    5,
				RequestRetryInterval: 2,
			},
			expectedErr: nil,
		},
		"invalid type": {
			config:         `max_request_retries: foo`,
			expectedConfig: defaultConfig,
			expectedErr:    &yaml.TypeError{Errors: []string{"line 1: cannot unmarshal !!str `foo` into int"}},
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
