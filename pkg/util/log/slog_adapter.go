package log

import (
	"log/slog"

	"github.com/go-kit/log"
	sloggk "github.com/tjhop/slog-gokit"
)

// GoKitLogToSlog convert go-kit/log to slog
// usage: logutil.GoKitLogToSlog(gokitLogger)
func GoKitLogToSlog(logger log.Logger) *slog.Logger {
	levelVar := slog.LevelVar{}
	promLogger, ok := logger.(*PrometheusLogger)
	if !ok {
		levelVar.Set(slog.LevelDebug)
	} else {
		switch promLogger.logLevel.String() {
		case "debug":
			levelVar.Set(slog.LevelDebug)
		case "info":
			levelVar.Set(slog.LevelInfo)
		case "warn":
			levelVar.Set(slog.LevelWarn)
		case "error":
			levelVar.Set(slog.LevelError)
		}
	}
	return slog.New(sloggk.NewGoKitHandler(logger, &levelVar))
}
