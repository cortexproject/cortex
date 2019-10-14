package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestParseConfig(t *testing.T) {
	conf := `
store: consul
consul:
  host: "consul:8500"
  consistentreads: true
prefix: "test/"
multi:
  primary: consul
  secondary: etcd
`

	cfg := Config{}

	err := yaml.Unmarshal([]byte(conf), &cfg)
	require.NoError(t, err)
	require.Equal(t, "consul", cfg.Store)
	require.Equal(t, "test/", cfg.Prefix)
	require.Equal(t, "consul:8500", cfg.Consul.Host)
	require.Equal(t, "consul", cfg.Multi.Primary)
	require.Equal(t, "etcd", cfg.Multi.Secondary)
}
