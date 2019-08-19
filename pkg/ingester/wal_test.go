package ingester

import (
	"io/ioutil"
	"os"
	"testing"

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

	// Build an ingester, add some samples, then shut it down.
	_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
	userIDs, testData := pushTestSamples(t, ing, 10, 1000, 0)
	ing.Shutdown()

	cfg.WALConfig.enabled = false
	// Start a new ingester and recover the WAL.
	_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
	defer ing.Shutdown()
	require.NoError(t, recoverFromWAL(ing))

	// Check the samples are still there!
	retrieveTestSamples(t, ing, userIDs, testData)
}
