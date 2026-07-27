package request_tracker

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
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPITracker(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "api-tracker-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tracker := NewRequestTracker(tmpDir, "apis.active", 10, logger)
	require.NotNil(t, tracker)
	defer tracker.Close()

	ctx := context.Background()
	insertIndex, err := tracker.Insert(ctx, []byte{})
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

	tracker := NewRequestTracker(tmpDir, "apis.active", 10, logger)
	require.NotNil(t, tracker)
	defer tracker.Close()
	output := logOutput.String()
	assert.Contains(t, output, "These requests didn't finish in cortex's last run")
	assert.Contains(t, output, "/api/v1/series")
}

func TestAPITrackerNilDirectory(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tracker := NewRequestTracker("", "apis.active", 10, logger)
	assert.Nil(t, tracker)
}

func TestAPIWrapper(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "api-wrapper-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	tracker := NewRequestTracker(tmpDir, "apis.active", 10, logger)
	require.NotNil(t, tracker)
	defer tracker.Close()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrapper := NewRequestWrapper(handler, tracker, &ApiExtractor{})

	req := httptest.NewRequest("GET", "/api/v1/series?match[]=up", nil)
	rr := httptest.NewRecorder()
	wrapper.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestAPIWrapperNilTracker(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrapper := NewRequestWrapper(handler, nil, &ApiExtractor{})

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

	tracker := NewRequestTracker(tmpDir, "apis.active", 2, logger)
	require.NotNil(t, tracker)
	defer tracker.Close()
	ctx := context.Background()

	index1, err := tracker.Insert(ctx, []byte{})
	require.NoError(t, err)

	index2, err := tracker.Insert(ctx, []byte{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err = tracker.Insert(ctx, []byte{})
	assert.Error(t, err) // Should timeout

	tracker.Delete(index1)
	ctx = context.Background()
	index3, err := tracker.Insert(ctx, []byte{})
	require.NoError(t, err)

	tracker.Delete(index2)
	tracker.Delete(index3)
}

// TestTrimForJsonMarshalMultiByteUTF8 ensures that truncating a string made of
// multi-byte UTF-8 characters never panics, even when the requested size is
// zero, negative, or lands in the middle of a multi-byte rune.
func TestTrimForJsonMarshalMultiByteUTF8(t *testing.T) {
	// "世" is a 3-byte UTF-8 character.
	multiByte := strings.Repeat("世", 10)

	tests := []struct {
		name string
		str  string
		size int
	}{
		{name: "negative size", str: multiByte, size: -5},
		{name: "zero size", str: multiByte, size: 0},
		{name: "size one (mid-rune)", str: multiByte, size: 1},
		{name: "size two (mid-rune)", str: multiByte, size: 2},
		{name: "size in middle of a rune", str: multiByte, size: 4},
		{name: "size larger than string", str: multiByte, size: 1000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				out := trimForJsonMarshal(tc.str, tc.size)
				// Result must always be valid UTF-8 (no partial runes).
				assert.True(t, utf8.ValidString(out), "result should be valid UTF-8")
				assert.LessOrEqual(t, len(out), len(tc.str))
			})
		})
	}
}

// TestGenerateJSONEntryWithTruncatedFieldNegativeSize reproduces the request
// tracker panic where a multi-byte UTF-8 field had to be truncated to a
// negative remaining size because the rest of the entry already consumed the
// available budget. This must not panic and must still produce valid JSON.
func TestGenerateJSONEntryWithTruncatedFieldNegativeSize(t *testing.T) {
	// Fill the base entry so the remaining budget for the field is <= 0.
	entryMap := make(map[string]any)
	entryMap["filler"] = strings.Repeat("a", maxEntrySize)

	require.NotPanics(t, func() {
		out := generateJSONEntryWithTruncatedField(entryMap, "query", strings.Repeat("世", 100))
		assert.NotNil(t, out)
	})
}

// TestRangedQueryExtractorMultiByteTruncation exercises the full extraction
// path with a large multi-byte UTF-8 query and oversized headers, forcing the
// query field to be truncated to a very small (potentially negative) budget.
func TestRangedQueryExtractorMultiByteTruncation(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/query_range?query="+strings.Repeat("世", 500), nil)
	// Large header value inflates the base entry, shrinking the budget left
	// for the multi-byte query field.
	req.Header.Set("User-Agent", strings.Repeat("x", maxEntrySize))

	extractor := &RangedQueryExtractor{}
	require.NotPanics(t, func() {
		entry := extractor.Extract(req)
		assert.True(t, utf8.Valid(entry), "entry should be valid UTF-8")
	})
}
