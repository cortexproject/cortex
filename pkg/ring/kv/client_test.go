package kv

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ring/kv/etcd"
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

func Test_createClient_multi(t *testing.T) {
	cfg := StoreConfig{
		Consul: consul.Config{
			Host: "consul.test",
		},
		Etcd: etcd.Config{
			Endpoints: []string{"etcd.test"},
		},
		Multi: MultiConfig{
			Primary:   "consul",
			Secondary: "etcd",
		},
	}
	require.NotPanics(t, func() {
		_, err := createClient("multi", "/collector", cfg, codec.NewProtoCodec("test", nil), prometheus.NewRegistry())
		require.NoError(t, err)
	})
}
