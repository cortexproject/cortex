package log

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
)

func Test_SlogAdapter_LogLevel(t *testing.T) {
	ctx := context.Background()
	logLevels := []string{"debug", "info", "warn", "error"}
	slogLevels := []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}

	for i, lv := range logLevels {
		cfg := &server.Config{}
		require.NoError(t, cfg.LogLevel.Set(lv))
		InitLogger(cfg)

		slog := GoKitLogToSlog(Logger)
		for j, slogLv := range slogLevels {
			if i <= j {
				t.Logf("[go-kit log level: %v, slog level: %v] slog should be enabled", lv, slogLv)
				require.True(t, slog.Enabled(ctx, slogLv))
			} else {
				t.Logf("[go-kit log level: %v, slog level: %v] slog should be disabled", lv, slogLv)
				require.False(t, slog.Enabled(ctx, slogLv))
			}
		}
	}
}
