package chunk

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAWSConfigFromURL(t *testing.T) {
	for _, tc := range []struct {
		url            string
		expectedKey    string
		expectedSecret string
		expectedRegion string
		expectedEp     string

		expectedNotSpecifiedUserErr bool
	}{
		{
			"s3://abc:123@s3.default.svc.cluster.local:4569",
			"abc",
			"123",
			"dummy",
			"http://s3.default.svc.cluster.local:4569",
			false,
		},
		{
			"dynamodb://user:pass@dynamodb.default.svc.cluster.local:8000/cortex",
			"user",
			"pass",
			"dummy",
			"http://dynamodb.default.svc.cluster.local:8000",
			false,
		},
		{
			// Not escaped password.
			"s3://abc:123/@s3.default.svc.cluster.local:4569",
			"",
			"",
			"",
			"",
			true,
		},
		{
			// Not escaped username.
			"s3://abc/:123@s3.default.svc.cluster.local:4569",
			"",
			"",
			"",
			"",
			true,
		},
		{
			"s3://keyWithEscapedSlashAtTheEnd%2F:%24%2C%26%2C%2B%2C%27%2C%2F%2C%3A%2C%3B%2C%3D%2C%3F%2C%40@eu-west-2/bucket1",
			"keyWithEscapedSlashAtTheEnd/",
			"$,&,+,',/,:,;,=,?,@",
			"eu-west-2",
			"",
			false,
		},
	} {
		parsedURL, err := url.Parse(tc.url)
		require.NoError(t, err)

		cfg, err := awsConfigFromURL(parsedURL)
		if tc.expectedNotSpecifiedUserErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)

		require.NotNil(t, cfg.Credentials)
		val, err := cfg.Credentials.Get()
		require.NoError(t, err)

		assert.Equal(t, tc.expectedKey, val.AccessKeyID)
		assert.Equal(t, tc.expectedSecret, val.SecretAccessKey)

		require.NotNil(t, cfg.Region)
		assert.Equal(t, tc.expectedRegion, *cfg.Region)

		if tc.expectedEp != "" {
			require.NotNil(t, cfg.Endpoint)
			assert.Equal(t, tc.expectedEp, *cfg.Endpoint)
		}
	}
}
