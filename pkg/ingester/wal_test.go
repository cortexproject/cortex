package ingester

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWAL(t *testing.T) {
	dirname, err := ioutil.TempDir("", "cortex-wal")
	require.NoError(t, err)

	cfg := defaultIngesterTestConfig()
	cfg.WALConfig.walEnabled = true
	cfg.WALConfig.checkpointEnabled = true
	cfg.WALConfig.recover = true
	cfg.WALConfig.dir = dirname
	cfg.WALConfig.checkpointDuration = 100 * time.Millisecond

	numSeries := 100
	numSamplesPerSeriesPerPush := 10
	numRestarts := 3

	// Build an ingester, add some samples, then shut it down.
	_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
	userIDs, testData := pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, 0)
	ing.Shutdown()

	for r := 0; r < numRestarts; r++ {
		if r == numRestarts-1 {
			cfg.WALConfig.walEnabled = false
			cfg.WALConfig.checkpointEnabled = false
		}
		// Start a new ingester and recover the WAL.
		_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())

		for i, userID := range userIDs {
			testData[userID] = buildTestMatrix(numSeries, (r+1)*numSamplesPerSeriesPerPush, i)
		}
		// Check the samples are still there!
		retrieveTestSamples(t, ing, userIDs, testData)

		if r != numRestarts-1 {
			userIDs, testData = pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, (r+1)*numSamplesPerSeriesPerPush)
		}

		ing.Shutdown()
	}
}
