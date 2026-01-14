package api_tracker

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPITracker(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "api-tracker-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tracker := NewAPITracker(tmpDir, 10, logger)
	require.NotNil(t, tracker)
	defer tracker.Close()

	ctx := context.Background()
	insertIndex, err := tracker.Insert(ctx, make(map[string]interface{}))
	require.NoError(t, err)
	assert.Greater(t, insertIndex, 0)

	tracker.Delete(insertIndex)
}

func TestAPITrackerLogUnfinished(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "api-tracker-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	filename := filepath.Join(tmpDir, "apis.active")
	content := `[{"path":"/api/v1/series","method":"GET","timestamp_sec":1234567890},`
	err = os.WriteFile(filename, []byte(content), 0644)
	require.NoError(t, err)

	var logOutput strings.Builder
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))

	tracker := NewAPITracker(tmpDir, 10, logger)
	require.NotNil(t, tracker)
	defer tracker.Close()
	output := logOutput.String()
	assert.Contains(t, output, "These API calls didn't finish in prometheus' last run")
	assert.Contains(t, output, "/api/v1/series")
}

func TestAPITrackerNilDirectory(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tracker := NewAPITracker("", 10, logger)
	assert.Nil(t, tracker)
}

func TestAPIWrapper(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "api-wrapper-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tracker := NewAPITracker(tmpDir, 10, logger)
	require.NotNil(t, tracker)
	defer tracker.Close()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrapper := NewAPIWrapper(handler, tracker)

	req := httptest.NewRequest("GET", "/api/v1/series?match[]=up", nil)
	rr := httptest.NewRecorder()
	wrapper.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestAPIWrapperNilTracker(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrapper := NewAPIWrapper(handler, nil)

	req := httptest.NewRequest("GET", "/api/v1/series?match[]=up", nil)
	rr := httptest.NewRecorder()
	wrapper.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestAPITrackerAboveMaxConcurrency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "api-tracker-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tracker := NewAPITracker(tmpDir, 2, logger)
	require.NotNil(t, tracker)
	defer tracker.Close()
	ctx := context.Background()

	index1, err := tracker.Insert(ctx, make(map[string]interface{}))
	require.NoError(t, err)

	index2, err := tracker.Insert(ctx, make(map[string]interface{}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err = tracker.Insert(ctx, make(map[string]interface{}))
	assert.Error(t, err) // Should timeout

	tracker.Delete(index1)
	ctx = context.Background()
	index3, err := tracker.Insert(ctx, make(map[string]interface{}))
	require.NoError(t, err)

	tracker.Delete(index2)
	tracker.Delete(index3)
}
