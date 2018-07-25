package util

import (
	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

var (
	// Logger is a shared go-kit logger.
	// TODO: Change all components to take a non-global logger via their constructors.
	Logger = log.NewNopLogger()

	logMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "log_messages_total",
		Help: "Total number of log messages.",
	}, []string{"level"})

	supportedLevels = []level.Value{
		level.DebugValue(),
		level.InfoValue(),
		level.WarnValue(),
		level.ErrorValue(),
	}
)

func init() {
	prometheus.MustRegister(logMessages)
}

// InitLogger initialises the global gokit logger (util.Logger) and overrides the
// default logger for the server.
func InitLogger(cfg *server.Config) {
	l, err := NewPrometheusLogger(cfg.LogLevel)
	if err != nil {
		panic(err)
	}

	Logger = l
	cfg.Log = logging.GoKit(l)
}

// PrometheusLogger exposes Prometheus counters for each of go-kit's log levels.
type PrometheusLogger struct {
	logger log.Logger
}

// NewPrometheusLogger creates a new instance of PrometheusLogger which exposes
// Prometheus counters for various log levels.
func NewPrometheusLogger(l logging.Level) (log.Logger, error) {
	// This code copy-pasted from prometheus/common/promlog.New()
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, l.Gokit)

	// Initialise counters for all supported levels:
	for _, level := range supportedLevels {
		logMessages.WithLabelValues(level.String())
	}

	logger = &PrometheusLogger{
		logger: logger,
	}

	// DefaultCaller must be the last wrapper
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	return logger, nil
}

// Log increments the appropriate Prometheus counter depending on the log level.
func (pl *PrometheusLogger) Log(kv ...interface{}) error {
	pl.logger.Log(kv...)
	l := "unknown"
	for i := 1; i < len(kv); i += 2 {
		if v, ok := kv[i].(level.Value); ok {
			l = v.String()
			break
		}
	}
	logMessages.WithLabelValues(l).Inc()
	return nil
}

// WithContext returns a Logger that has information about the current user in
// its details.
//
// e.g.
//   log := util.WithContext(ctx)
//   log.Errorf("Could not chunk chunks: %v", err)
func WithContext(ctx context.Context, l log.Logger) log.Logger {
	// Weaveworks uses "orgs" and "orgID" to represent Cortex users,
	// even though the code-base generally uses `userID` to refer to the same thing.
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return l
	}
	return WithUserID(userID, l)
}

// WithUserID returns a Logger that has information about the current user in
// its details.
func WithUserID(userID string, l log.Logger) log.Logger {
	// See note in WithContext.
	return log.With(l, "org_id", userID)
}
