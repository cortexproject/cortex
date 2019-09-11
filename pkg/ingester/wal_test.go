package ingester

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
)

func init() {
	util.Logger = log.NewLogfmtLogger(os.Stdout)
}

func TestWAL(t *testing.T) {
	dirname, err := ioutil.TempDir("", "cortex-wal")
	require.NoError(t, err)

	cfg := defaultIngesterTestConfig()
	cfg.WALConfig.enabled = true
	cfg.WALConfig.dir = dirname
	cfg.WALConfig.checkpointDuration = 10 * time.Millisecond

	numSeries := 10
	numSamplesPerSeriesPerPush := 1000
	numRestarts := 5

	// Build an ingester, add some samples, then shut it down.
	_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
	userIDs, testData := pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, 0)
	ing.Shutdown()

	for r := 0; r < numRestarts; r++ {
		// Start a new ingester and recover the WAL.
		_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
		require.NoError(t, recoverFromWAL(ing))

		for i, userID := range userIDs {
			testData[userID] = buildTestMatrix(numSeries, (r+1)*numSamplesPerSeriesPerPush, i)
		}
		// Check the samples are still there!
		retrieveTestSamples(t, ing, userIDs, testData)
		userIDs, testData = pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, (r+1)*numSamplesPerSeriesPerPush)
		ing.Shutdown()
	}

	_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
	defer ing.Shutdown()
	require.NoError(t, recoverFromWAL(ing))

	for i, userID := range userIDs {
		testData[userID] = buildTestMatrix(numSeries, (numRestarts+1)*numSamplesPerSeriesPerPush, i)
	}
	retrieveTestSamples(t, ing, userIDs, testData)
}
