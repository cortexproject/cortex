package users

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestCachedScanner_ScanUsers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := map[string]struct {
		scanner       *mockScanner
		ttl           time.Duration
		waitTime      time.Duration
		expectedCalls int
		expectErr     bool
	}{
		"cache hit within TTL": {
			scanner: &mockScanner{
				active:   []string{"user-1"},
				deleting: []string{"user-2"},
				deleted:  []string{"user-3"},
			},
			ttl:           1 * time.Hour,
			waitTime:      0,
			expectedCalls: 1,
			expectErr:     false,
		},
		"cache miss after TTL": {
			scanner: &mockScanner{
				active:   []string{"user-1"},
				deleting: []string{"user-2"},
				deleted:  []string{"user-3"},
			},
			ttl:           100 * time.Millisecond,
			waitTime:      500 * time.Millisecond,
			expectedCalls: 2,
			expectErr:     false,
		},
		"scanner error": {
			scanner: &mockScanner{
				err: assert.AnError,
			},
			ttl:           1 * time.Hour,
			waitTime:      0,
			expectedCalls: 1,
			expectErr:     true,
		},
		"empty results": {
			scanner: &mockScanner{
				active:   []string{},
				deleting: []string{},
				deleted:  []string{},
			},
			ttl:           1 * time.Hour,
			waitTime:      0,
			expectedCalls: 1,
			expectErr:     false,
		},
	}

	for testName, testData := range tests {
		testData := testData
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			reg := prometheus.NewRegistry()
			cachedScanner := newCachedScanner(testData.scanner, tsdb.UsersScannerConfig{
				CacheTTL: testData.ttl,
			}, reg)

			// First call
			active, deleting, deleted, err := cachedScanner.ScanUsers(ctx)
			if testData.expectErr {
				require.Error(t, err)
				assert.Equal(t, assert.AnError, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, testData.scanner.active, active)
			assert.Equal(t, testData.scanner.deleting, deleting)
			assert.Equal(t, testData.scanner.deleted, deleted)

			// Wait if needed
			if testData.waitTime > 0 {
				time.Sleep(testData.waitTime)
			}

			// Second call
			active, deleting, deleted, err = cachedScanner.ScanUsers(ctx)
			if testData.expectErr {
				require.Error(t, err)
				assert.Equal(t, assert.AnError, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, testData.scanner.active, active)
			assert.Equal(t, testData.scanner.deleting, deleting)
			assert.Equal(t, testData.scanner.deleted, deleted)

			// Verify number of calls to underlying scanner
			assert.Equal(t, testData.expectedCalls, testData.scanner.calls)
		})
	}
}

func TestCachedScanner_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reg := prometheus.NewRegistry()
	scanner := &mockScanner{
		active:   []string{"user-1"},
		deleting: []string{"user-2"},
		deleted:  []string{"user-3"},
	}

	cachedScanner := newCachedScanner(scanner, tsdb.UsersScannerConfig{
		CacheTTL: 1 * time.Hour,
	}, reg)

	// Run multiple concurrent scans
	const goroutines = 10
	done := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()

			active, deleting, deleted, err := cachedScanner.ScanUsers(ctx)
			require.NoError(t, err)
			assert.Equal(t, scanner.active, active)
			assert.Equal(t, scanner.deleting, deleting)
			assert.Equal(t, scanner.deleted, deleted)
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Verify that the underlying scanner was called only once
	assert.Equal(t, 1, scanner.calls)
}
