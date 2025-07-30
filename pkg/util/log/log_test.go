package log

import (
	"io"
	"os"
	"testing"

	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
)

func TestInitLogger(t *testing.T) {
	stderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	defer func() { os.Stderr = stderr }()

	cfg := &server.Config{}
	require.NoError(t, cfg.LogLevel.Set("debug"))
	InitLogger(cfg)

	level.Debug(Logger).Log("hello", "world")
	cfg.Log.Debugf("%s %s", "hello", "world")

	require.NoError(t, w.Close())
	logs, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Contains(t, string(logs), "caller=log_test.go:24 level=debug hello=world")
	require.Contains(t, string(logs), "caller=log_test.go:25 level=debug msg=\"hello world\"")
}

func BenchmarkDisallowedLogLevels(b *testing.B) {
	cfg := &server.Config{}
	require.NoError(b, cfg.LogLevel.Set("warn"))
	InitLogger(cfg)

	for i := 0; i < b.N; i++ {
		level.Info(Logger).Log("hello", "world", "number", i)
		level.Debug(Logger).Log("hello", "world", "number", i)
	}
}
