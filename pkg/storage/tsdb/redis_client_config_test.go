package tsdb

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/tls"
)

func TestRedisIndexCacheConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		conf *RedisClientConfig
		err  error
	}{
		{
			name: "empty address",
			conf: &RedisClientConfig{},
			err:  errNoIndexCacheAddresses,
		},
		{
			name: "provide TLS cert path but not key path",
			conf: &RedisClientConfig{
				Addresses:  "aaa",
				TLSEnabled: true,
				TLS: tls.ClientConfig{
					CertPath: "foo",
				},
			},
			err: errors.New("both client key and certificate must be provided"),
		},
		{
			name: "provide TLS key path but not cert path",
			conf: &RedisClientConfig{
				Addresses:  "aaa",
				TLSEnabled: true,
				TLS: tls.ClientConfig{
					KeyPath: "foo",
				},
			},
			err: errors.New("both client key and certificate must be provided"),
		},
		{
			name: "success when TLS enabled",
			conf: &RedisClientConfig{
				Addresses:  "aaa",
				TLSEnabled: true,
				TLS: tls.ClientConfig{
					KeyPath:  "foo",
					CertPath: "bar",
				},
			},
		},
		{
			name: "success when TLS disabled",
			conf: &RedisClientConfig{
				Addresses:  "aaa",
				TLSEnabled: false,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.conf.Validate()
			if tc.err == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.err.Error(), err.Error())
			}
		})
	}
}
