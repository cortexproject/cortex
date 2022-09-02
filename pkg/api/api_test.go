package api

import (
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
)

type FakeLogger struct{}

func (fl *FakeLogger) Log(keyvals ...interface{}) error {
	return nil
}

func TestNewApiWithoutSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		MetricsNamespace:  "without_source_ip_extractor",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.Nil(t, api.sourceIPs)
}

func TestNewApiWithSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		LogSourceIPs:      true,
		MetricsNamespace:  "with_source_ip_extractor",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.NotNil(t, api.sourceIPs)
}

func TestNewApiWithInvalidSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	s := server.Server{
		HTTP: &mux.Router{},
	}
	serverCfg := server.Config{
		HTTPListenNetwork:  server.DefaultNetwork,
		LogSourceIPs:       true,
		LogSourceIPsHeader: "SomeHeader",
		LogSourceIPsRegex:  "[*",
		MetricsNamespace:   "with_invalid_source_ip_extractor",
	}

	api, err := New(cfg, serverCfg, &s, &FakeLogger{})
	require.Error(t, err)
	require.Nil(t, api)
}

func TestNewApiWithHeaderLogging(t *testing.T) {
	cfg := Config{
		HTTPRequestHeadersToLog: []string{"ForTesting"},
	}
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		MetricsNamespace:  "with_header_logging",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.NotNil(t, api.HTTPHeaderMiddleware)

}

func TestNewApiWithoutHeaderLogging(t *testing.T) {
	cfg := Config{
		HTTPRequestHeadersToLog: []string{},
	}
	serverCfg := server.Config{
		HTTPListenNetwork: server.DefaultNetwork,
		MetricsNamespace:  "without_header_logging",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.Nil(t, api.HTTPHeaderMiddleware)

}
