package ingester

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestWAL(t *testing.T) {
	dirname, err := ioutil.TempDir("", "cortex-wal")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(dirname))
	}()

	cfg := defaultIngesterTestConfig(t)
	cfg.WALConfig.WALEnabled = true
	cfg.WALConfig.CheckpointEnabled = true
	cfg.WALConfig.Recover = true
	cfg.WALConfig.Dir = dirname
	cfg.WALConfig.CheckpointDuration = 100 * time.Minute
	cfg.WALConfig.checkpointDuringShutdown = true

	numSeries := 100
	numSamplesPerSeriesPerPush := 10
	numRestarts := 5

	// Build an ingester, add some samples, then shut it down.
	_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)
	userIDs, testData := pushTestSamples(t, ing, numSeries, numSamplesPerSeriesPerPush, 0)
	// Checkpoint happens when stopping.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))

	for r := 0; r < numRestarts; r++ {
		if r == 2 {
			// From 3rd restart onwards, we are disabling checkpointing during shutdown
			// to test both checkpoint+WAL replay.
			cfg.WALConfig.checkpointDuringShutdown = false
		}
		if r == numRestarts-1 {
			cfg.WALConfig.WALEnabled = false
			cfg.WALConfig.CheckpointEnabled = false
		}

		// Start a new ingester and recover the WAL.
		_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)

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

	cfg.WALConfig.WALEnabled = true
	cfg.WALConfig.CheckpointEnabled = true

	// Start a new ingester and recover the WAL.
	_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)

	userID := userIDs[0]
	sampleStream := testData[userID][0]
	lastSample := sampleStream.Values[len(sampleStream.Values)-1]

	// In-order and out of order sample in the same request.
	metric := cortexpb.FromLabelAdaptersToLabels(cortexpb.FromMetricsToLabelAdapters(sampleStream.Metric))
	outOfOrderSample := cortexpb.Sample{TimestampMs: int64(lastSample.Timestamp - 10), Value: 99}
	inOrderSample := cortexpb.Sample{TimestampMs: int64(lastSample.Timestamp + 10), Value: 999}

	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = ing.Push(ctx, cortexpb.ToWriteRequest(
		[]labels.Labels{metric, metric},
		[]cortexpb.Sample{outOfOrderSample, inOrderSample}, nil, cortexpb.API))
	require.Equal(t, httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(makeMetricValidationError(sampleOutOfOrder, metric,
		fmt.Errorf("sample timestamp out of order; last timestamp: %v, incoming timestamp: %v", lastSample.Timestamp, model.Time(outOfOrderSample.TimestampMs))), userID).Error()), err)

	// We should have logged the in-order sample.
	testData[userID][0].Values = append(testData[userID][0].Values, model.SamplePair{
		Timestamp: model.Time(inOrderSample.TimestampMs),
		Value:     model.SampleValue(inOrderSample.Value),
	})

	// Check samples after restart from WAL.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)
	retrieveTestSamples(t, ing, userIDs, testData)
}

func TestCheckpointRepair(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
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
		_, ing := newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)

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
		_, ing = newTestStore(t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)
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

func TestCheckpointIndex(t *testing.T) {
	tcs := []struct {
		filename    string
		includeTmp  bool
		index       int
		shouldError bool
	}{
		{
			filename:    "checkpoint.123456",
			includeTmp:  false,
			index:       123456,
			shouldError: false,
		},
		{
			filename:    "checkpoint.123456",
			includeTmp:  true,
			index:       123456,
			shouldError: false,
		},
		{
			filename:    "checkpoint.123456.tmp",
			includeTmp:  true,
			index:       123456,
			shouldError: false,
		},
		{
			filename:    "checkpoint.123456.tmp",
			includeTmp:  false,
			shouldError: true,
		},
		{
			filename:    "not-checkpoint.123456.tmp",
			includeTmp:  true,
			shouldError: true,
		},
		{
			filename:    "checkpoint.123456.tmp2",
			shouldError: true,
		},
		{
			filename:    "checkpoints123456",
			shouldError: true,
		},
		{
			filename:    "012345",
			shouldError: true,
		},
	}
	for _, tc := range tcs {
		index, err := checkpointIndex(tc.filename, tc.includeTmp)
		if tc.shouldError {
			require.Error(t, err, "filename: %s, includeTmp: %t", tc.filename, tc.includeTmp)
			continue
		}

		require.NoError(t, err, "filename: %s, includeTmp: %t", tc.filename, tc.includeTmp)
		require.Equal(t, tc.index, index)
	}
}

func BenchmarkWALReplay(b *testing.B) {
	dirname, err := ioutil.TempDir("", "cortex-wal")
	require.NoError(b, err)
	defer func() {
		require.NoError(b, os.RemoveAll(dirname))
	}()

	cfg := defaultIngesterTestConfig(b)
	cfg.WALConfig.WALEnabled = true
	cfg.WALConfig.CheckpointEnabled = true
	cfg.WALConfig.Recover = true
	cfg.WALConfig.Dir = dirname
	cfg.WALConfig.CheckpointDuration = 100 * time.Minute
	cfg.WALConfig.checkpointDuringShutdown = false

	numSeries := 10
	numSamplesPerSeriesPerPush := 2
	numPushes := 100000

	_, ing := newTestStore(b, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)

	// Add samples for the checkpoint.
	for r := 0; r < numPushes; r++ {
		_, _ = pushTestSamples(b, ing, numSeries, numSamplesPerSeriesPerPush, r*numSamplesPerSeriesPerPush)
	}
	w, ok := ing.wal.(*walWrapper)
	require.True(b, ok)
	require.NoError(b, w.performCheckpoint(true))

	// Add samples for the additional WAL not in checkpoint.
	for r := 0; r < numPushes; r++ {
		_, _ = pushTestSamples(b, ing, numSeries, numSamplesPerSeriesPerPush, (numPushes+r)*numSamplesPerSeriesPerPush)
	}

	require.NoError(b, services.StopAndAwaitTerminated(context.Background(), ing))

	var ing2 *Ingester
	b.Run("wal replay", func(b *testing.B) {
		// Replay will happen here.
		_, ing2 = newTestStore(b, cfg, defaultClientTestConfig(), defaultLimitsTestConfig(), nil)
	})
	require.NoError(b, services.StopAndAwaitTerminated(context.Background(), ing2))
}
