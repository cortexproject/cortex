package ingester

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestWAL(t *testing.T) {
	dirname, err := ioutil.TempDir("", "cortex-wal")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dirname))
	}()

	cfg := defaultIngesterTestConfig()
	cfg.WALConfig.WALEnabled = true
	cfg.WALConfig.CheckpointEnabled = true
	cfg.WALConfig.Recover = true
	cfg.WALConfig.Dir = dirname
	cfg.WALConfig.CheckpointDuration = 100 * time.Millisecond

	numSeries := 100
	numSamplesPerSeriesPerPush := 10
	numRestarts := 3

	// Build an ingester, add some samples, then shut it down.
	_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
	userIDs, testData := pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, 0)
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))

	for r := 0; r < numRestarts; r++ {
		if r == numRestarts-1 {
			cfg.WALConfig.WALEnabled = false
			cfg.WALConfig.CheckpointEnabled = false
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

		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	}
}

func TestCheckpointRepair(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.WALConfig.WALEnabled = true
	cfg.WALConfig.CheckpointEnabled = true
	cfg.WALConfig.Recover = true
	cfg.WALConfig.CheckpointDuration = 100 * time.Hour // Basically no automatic checkpoint.

	numSeries := 100
	numSamplesPerSeriesPerPush := 10
	for _, numCheckpoints := range []int{0, 1, 2, 3} {
		dirname, err := ioutil.TempDir("", "cortex-wal")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.RemoveAll(dirname))
		}()
		cfg.WALConfig.Dir = dirname

		// Build an ingester, add some samples, then shut it down.
		_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())

		w, ok := ing.wal.(*walWrapper)
		require.True(t, ok)

		var userIDs []string
		// Push some samples for the 0 checkpoints case.
		// We dont shutdown the ingester in that case, else it will create a checkpoint.
		userIDs, _ = pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, 0)
		for i := 0; i < numCheckpoints; i++ {
			// First checkpoint.
			userIDs, _ = pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, (i+1)*numSamplesPerSeriesPerPush)
			if i == numCheckpoints-1 {
				// Shutdown creates a checkpoint. This is only for the last checkpoint.
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
			} else {
				require.NoError(t, w.performCheckpoint(true))
			}
		}

		require.Equal(t, float64(numCheckpoints), prom_testutil.ToFloat64(w.checkpointCreationTotal))

		// Verify checkpoint dirs.
		files, err := ioutil.ReadDir(w.wal.Dir())
		require.NoError(t, err)
		numDirs := 0
		for _, f := range files {
			if f.IsDir() {
				numDirs++
			}
		}
		if numCheckpoints <= 1 {
			require.Equal(t, numCheckpoints, numDirs)
		} else {
			// At max there are last 2 checkpoints on the disk.
			require.Equal(t, 2, numDirs)
		}

		if numCheckpoints > 0 {
			// Corrupt the last checkpoint.
			lastChDir, _, err := lastCheckpoint(w.wal.Dir())
			require.NoError(t, err)
			files, err = ioutil.ReadDir(lastChDir)
			require.NoError(t, err)

			lastFile, err := os.OpenFile(filepath.Join(lastChDir, files[len(files)-1].Name()), os.O_WRONLY, os.ModeAppend)
			require.NoError(t, err)
			n, err := lastFile.WriteAt([]byte{1, 2, 3, 4}, 2)
			require.NoError(t, err)
			require.Equal(t, 4, n)
			require.NoError(t, lastFile.Close())
		}

		// Open an ingester for the repair.
		_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())
		w, ok = ing.wal.(*walWrapper)
		require.True(t, ok)
		// defer in case we hit an error though we explicitly close it later.
		defer func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
		}()

		if numCheckpoints > 0 {
			require.Equal(t, 1.0, prom_testutil.ToFloat64(ing.metrics.walCorruptionsTotal))
		} else {
			require.Equal(t, 0.0, prom_testutil.ToFloat64(ing.metrics.walCorruptionsTotal))
		}

		// Verify checkpoint dirs after the corrupt checkpoint is deleted.
		files, err = ioutil.ReadDir(w.wal.Dir())
		require.NoError(t, err)
		numDirs = 0
		for _, f := range files {
			if f.IsDir() {
				numDirs++
			}
		}
		if numCheckpoints <= 1 {
			// The only checkpoint is removed (or) there was no checkpoint at all.
			require.Equal(t, 0, numDirs)
		} else {
			// There is at max last 2 checkpoints. Hence only 1 should be remaining.
			require.Equal(t, 1, numDirs)
		}

		testData := map[string]model.Matrix{}
		// Verify we did not lose any data.
		for i, userID := range userIDs {
			// 'numCheckpoints*' because we ingested the data 'numCheckpoints' number of time.
			testData[userID] = buildTestMatrix(numSeries, (numCheckpoints+1)*numSamplesPerSeriesPerPush, i)
		}
		retrieveTestSamples(t, ing, userIDs, testData)

		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	}

}
