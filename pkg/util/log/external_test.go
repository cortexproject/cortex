package log

import (
	"bytes"
	"encoding/json"
	"testing"

	kitlog "github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/logging"
)

func TestGRPCLoggerUsesConfiguredLogger(t *testing.T) {
	t.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "")
	t.Setenv("GRPC_GO_LOG_VERBOSITY_LEVEL", "")

	logger, logs := testJSONLogger(t)
	grpcLogger := NewGRPCLogger(logger)

	grpcLogger.Info("not logged by default")
	grpcLogger.Error("transport failure")

	entry := readSingleJSONLogEntry(t, logs)
	require.Equal(t, "error", entry["level"])
	require.Equal(t, "transport failure", entry["msg"])
	require.NotContains(t, logs.String(), "not logged by default")
}

func TestAutomaxprocsLoggerUsesConfiguredLogger(t *testing.T) {
	logger, logs := testJSONLogger(t)

	AutomaxprocsLogger(logger)("maxprocs: Leaving GOMAXPROCS=%v: CPU quota undefined", 4)

	entry := readSingleJSONLogEntry(t, logs)
	require.Equal(t, "info", entry["level"])
	require.Equal(t, "maxprocs: Leaving GOMAXPROCS=4: CPU quota undefined", entry["msg"])
}

func testJSONLogger(t *testing.T) (kitlog.Logger, *bytes.Buffer) {
	t.Helper()

	var logLevel logging.Level
	require.NoError(t, logLevel.Set("debug"))

	logs := &bytes.Buffer{}
	return newPrometheusLoggerFrom(kitlog.NewJSONLogger(logs), logLevel), logs
}

func readSingleJSONLogEntry(t *testing.T, logs *bytes.Buffer) map[string]interface{} {
	t.Helper()

	lines := bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n"))
	require.Len(t, lines, 1)

	var entry map[string]interface{}
	require.NoError(t, json.Unmarshal(lines[0], &entry))
	return entry
}
