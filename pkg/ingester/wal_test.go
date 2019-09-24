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
			cfg.WALConfig.enabled = false
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
