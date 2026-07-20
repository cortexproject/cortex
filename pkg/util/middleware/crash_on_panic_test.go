package middleware

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCrashOnPanic_NoPanic(t *testing.T) {
	logger := log.NewNopLogger()

	handler := CrashOnPanic(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodPost, "/push", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())
}

func TestCrashOnPanic_WithPanic(t *testing.T) {
	// os.Exit cannot be tested directly. Use the subprocess test pattern:
	// run this test binary with a special env var, and verify the exit code.
	if os.Getenv("TEST_CRASH_ON_PANIC") == "1" {
		logger := log.NewNopLogger()

		handler := CrashOnPanic(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			panic("runtime error: growslice: len out of range")
		}))

		req := httptest.NewRequest(http.MethodPost, "/push", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		// Should not reach here.
		return
	}

	// Re-run this test in a subprocess with the env var set.
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashOnPanic_WithPanic")
	cmd.Env = append(os.Environ(), "TEST_CRASH_ON_PANIC=1")
	err := cmd.Run()

	// The subprocess should exit with a non-zero exit code.
	require.Error(t, err)

	exitErr, ok := err.(*exec.ExitError)
	require.True(t, ok, "expected *exec.ExitError, got %T", err)
	assert.False(t, exitErr.Success(), "process should have exited with non-zero status")
}
