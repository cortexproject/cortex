package log

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc/grpclog"
)

// InitExternalLoggers configures package-level loggers from dependencies that
// do not take Cortex's logger through normal constructors.
func InitExternalLoggers() {
	grpclog.SetLoggerV2(NewGRPCLogger(Logger))
}

// AutomaxprocsLogger adapts the printf-style logger used by automaxprocs to
// Cortex's configured go-kit logger.
func AutomaxprocsLogger(logger kitlog.Logger) func(string, ...interface{}) {
	return func(format string, args ...interface{}) {
		level.Info(logger).Log("msg", fmt.Sprintf(format, args...))
	}
}

// NewGRPCLogger adapts gRPC's package-level logger to Cortex's configured
// go-kit logger so gRPC transport logs use the selected Cortex log format.
func NewGRPCLogger(logger kitlog.Logger) grpclog.LoggerV2 {
	return &grpcLogger{
		logger:   logger,
		severity: grpcSeverityFromEnv(),
		verbose:  grpcVerbosityFromEnv(),
	}
}

type grpcSeverity int

const (
	grpcSeverityInfo grpcSeverity = iota
	grpcSeverityWarning
	grpcSeverityError
)

type grpcLogger struct {
	logger   kitlog.Logger
	severity grpcSeverity
	verbose  int
}

func (l *grpcLogger) Info(args ...any) {
	l.log(grpcSeverityInfo, fmt.Sprint(args...))
}

func (l *grpcLogger) Infoln(args ...any) {
	l.log(grpcSeverityInfo, trimPrintln(fmt.Sprintln(args...)))
}

func (l *grpcLogger) Infof(format string, args ...any) {
	l.log(grpcSeverityInfo, fmt.Sprintf(format, args...))
}

func (l *grpcLogger) Warning(args ...any) {
	l.log(grpcSeverityWarning, fmt.Sprint(args...))
}

func (l *grpcLogger) Warningln(args ...any) {
	l.log(grpcSeverityWarning, trimPrintln(fmt.Sprintln(args...)))
}

func (l *grpcLogger) Warningf(format string, args ...any) {
	l.log(grpcSeverityWarning, fmt.Sprintf(format, args...))
}

func (l *grpcLogger) Error(args ...any) {
	l.log(grpcSeverityError, fmt.Sprint(args...))
}

func (l *grpcLogger) Errorln(args ...any) {
	l.log(grpcSeverityError, trimPrintln(fmt.Sprintln(args...)))
}

func (l *grpcLogger) Errorf(format string, args ...any) {
	l.log(grpcSeverityError, fmt.Sprintf(format, args...))
}

func (l *grpcLogger) Fatal(args ...any) {
	l.log(grpcSeverityError, fmt.Sprint(args...))
	os.Exit(1)
}

func (l *grpcLogger) Fatalln(args ...any) {
	l.log(grpcSeverityError, trimPrintln(fmt.Sprintln(args...)))
	os.Exit(1)
}

func (l *grpcLogger) Fatalf(format string, args ...any) {
	l.log(grpcSeverityError, fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (l *grpcLogger) V(level int) bool {
	return level <= l.verbose
}

func (l *grpcLogger) InfoDepth(_ int, args ...any) {
	l.Infoln(args...)
}

func (l *grpcLogger) WarningDepth(_ int, args ...any) {
	l.Warningln(args...)
}

func (l *grpcLogger) ErrorDepth(_ int, args ...any) {
	l.Errorln(args...)
}

func (l *grpcLogger) FatalDepth(_ int, args ...any) {
	l.Fatalln(args...)
}

func (l *grpcLogger) log(severity grpcSeverity, msg string) {
	if severity < l.severity {
		return
	}

	switch severity {
	case grpcSeverityInfo:
		level.Info(l.logger).Log("msg", msg)
	case grpcSeverityWarning:
		level.Warn(l.logger).Log("msg", msg)
	default:
		level.Error(l.logger).Log("msg", msg)
	}
}

func grpcSeverityFromEnv() grpcSeverity {
	switch strings.ToLower(os.Getenv("GRPC_GO_LOG_SEVERITY_LEVEL")) {
	case "info":
		return grpcSeverityInfo
	case "warning":
		return grpcSeverityWarning
	default:
		return grpcSeverityError
	}
}

func grpcVerbosityFromEnv() int {
	verbosity, err := strconv.Atoi(os.Getenv("GRPC_GO_LOG_VERBOSITY_LEVEL"))
	if err != nil {
		return 0
	}
	return verbosity
}

func trimPrintln(msg string) string {
	return strings.TrimSuffix(msg, "\n")
}
