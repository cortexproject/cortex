// Package kslog provides a plug-in kgo.Logger wrapping slog.Logger for usage in
// a kgo.Client.
//
// This can be used like so:
//
//	cl, err := kgo.NewClient(
//	        kgo.WithLogger(kslog.New(slog.Default())),
//	        // ...other opts
//	)
package kslog

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Logger provides the kgo.Logger interface for usage in kgo.WithLogger when
// initializing a client.
type Logger struct {
	sl *slog.Logger
}

// New returns a new kgo.Logger that wraps an slog.Logger.
func New(sl *slog.Logger) *Logger {
	return &Logger{sl}
}

// Level is for the kgo.Logger interface.
func (l *Logger) Level() kgo.LogLevel {
	ctx := context.Background()
	switch {
	case l.sl.Enabled(ctx, slog.LevelDebug):
		return kgo.LogLevelDebug
	case l.sl.Enabled(ctx, slog.LevelInfo):
		return kgo.LogLevelInfo
	case l.sl.Enabled(ctx, slog.LevelWarn):
		return kgo.LogLevelWarn
	case l.sl.Enabled(ctx, slog.LevelError):
		return kgo.LogLevelError
	default:
		return kgo.LogLevelNone
	}
}

// Log is for the kgo.Logger interface.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	l.sl.Log(context.Background(), kgoToSlogLevel(level), msg, keyvals...)
}

func kgoToSlogLevel(level kgo.LogLevel) slog.Level {
	switch level {
	case kgo.LogLevelError:
		return slog.LevelError
	case kgo.LogLevelWarn:
		return slog.LevelWarn
	case kgo.LogLevelInfo:
		return slog.LevelInfo
	case kgo.LogLevelDebug:
		return slog.LevelDebug
	default:
		// Using the default level for slog
		return slog.LevelInfo
	}
}
