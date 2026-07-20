package middleware

import (
	"net/http"
	"os"
	"runtime"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// CrashOnPanic wraps an http.Handler and terminates the process if the handler
// panics. This is intended for stateless components (e.g., distributors) where
// a panic may leave the process in a corrupted state. Rather than continuing to
// serve bad traffic, the process exits and allows the orchestrator (e.g.,
// Kubernetes) to restart it with a clean state.
func CrashOnPanic(logger log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer crashOnPanic(logger)
		next.ServeHTTP(w, r)
	})
}

func crashOnPanic(logger log.Logger) {
	p := recover()
	if p == nil {
		return
	}

	// Capture the stack trace before exiting.
	const stackSize = 8192
	stack := make([]byte, stackSize)
	n := runtime.Stack(stack, false)

	level.Error(logger).Log(
		"msg", "panic detected, crashing process",
		"panic", p,
		"stack", string(stack[:n]),
	)

	// Flush stderr so the stack trace is visible in logs before exit.
	os.Stderr.Sync() //nolint:errcheck
	os.Exit(1)
}
