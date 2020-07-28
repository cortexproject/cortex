package ingester

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var singleTestLabel = []labels.Labels{[]labels.Label{{Name: "__name__", Value: "test"}}}

// This test case demonstrates problem with losing incoming samples while chunks are flushed with "immediate" mode.
func TestSweepImmediateDropsSamples(t *testing.T) {
	cfg := emptyIngesterConfig()
	cfg.FlushCheckPeriod = 1 * time.Minute
	cfg.RetainPeriod = 10 * time.Millisecond

	st := &sleepyCountingStore{}
	ing := createTestIngester(t, cfg, st)

	samples := newSampleGenerator(t, time.Now(), time.Millisecond)

	// Generates one sample.
	pushSample(t, ing, <-samples)

	notify := make(chan struct{})
	ing.preFlushChunks = func() {
		if ing.State() == services.Running {
			pushSample(t, ing, <-samples)
			notify <- struct{}{}
		}
	}

	// Simulate /flush. Sweeps everything, but also pushes another sample (in preFlushChunks)
	ing.sweepUsers(true)
	<-notify // Wait for flushing to happen.

	// Stopping ingester should sweep the remaining samples.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))

	require.Equal(t, 2, st.samples)
}

// There are several factors in this panic:
// Chunk is first flushed normally
// "/flush" is called (sweepUsers(true)), and that causes new flush of already flushed chunks
// During the flush to store (in flushChunks), chunk is actually removed from list of chunks (and its reference is niled) in removeFlushedChunks.
// After flushing to store, reference is nil, causing panic.
func TestFlushPanicIssue2743(t *testing.T) {
	cfg := emptyIngesterConfig()
	cfg.FlushCheckPeriod = 50 * time.Millisecond // We want to check for flush-able and removable chunks often.
	cfg.RetainPeriod = 500 * time.Millisecond    // Remove flushed chunks quickly. This triggers nil-ing. To get a panic, it should happen while Store is "writing" chunks. (We use "sleepy store" to enforce that)
	cfg.MaxChunkAge = 1 * time.Hour              // We don't use max chunk age for this test, as that is jittered.
	cfg.MaxChunkIdle = 200 * time.Millisecond    // Flush chunk 200ms after adding last sample.

	st := &sleepyCountingStore{d: 1 * time.Second} // Longer than retain period

	ing := createTestIngester(t, cfg, st)
	samples := newSampleGenerator(t, time.Now(), 1*time.Second)

	notifyCh := make(chan bool, 10)
	ing.preFlushChunks = func() {
		select {
		case notifyCh <- true:
		default:
		}
	}

	// Generates one sample
	pushSample(t, ing, <-samples)

	// Wait until regular flush kicks in (flushing due to chunk being idle)
	<-notifyCh

	// Sweep again -- this causes the same chunks to be queued for flushing again.
	// We must hit this *before* flushed chunk is removed from list of chunks. (RetainPeriod)
	// While store is flushing (simulated by sleep in the store), previously flushed chunk is removed from memory.
	ing.sweepUsers(true)

	// Wait a bit for flushing to end. In buggy version, we get panic while waiting.
	time.Sleep(2 * time.Second)
}

func pushSample(t *testing.T, ing *Ingester, sample client.Sample) {
	_, err := ing.Push(user.InjectOrgID(context.Background(), userID), client.ToWriteRequest(singleTestLabel, []client.Sample{sample}, nil, client.API))
	require.NoError(t, err)
}

func createTestIngester(t *testing.T, cfg Config, store ChunkStore) *Ingester {
	l := validation.Limits{}
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)

	ing, err := New(cfg, client.Config{}, overrides, store, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), ing)
	})

	return ing
}

type sleepyCountingStore struct {
	d       time.Duration
	samples int
}

func (m *sleepyCountingStore) Put(_ context.Context, chunks []chunk.Chunk) error {
	if m.d > 0 {
		time.Sleep(m.d)
	}

	for _, c := range chunks {
		m.samples += c.Data.Len()
	}
	return nil
}

func emptyIngesterConfig() Config {
	return Config{
		WALConfig: WALConfig{},
		LifecyclerConfig: ring.LifecyclerConfig{
			RingConfig: ring.Config{
				KVStore: kv.Config{
					Store: "inmemory",
				},
				ReplicationFactor: 1,
			},
			InfNames:        []string{"en0", "eth0", "lo0", "lo"},
			HeartbeatPeriod: 10 * time.Second,
		},

		ConcurrentFlushes: 1,             // Single queue only. Doesn't really matter for this test (same series is always flushed by same worker), but must be positive.
		RateUpdatePeriod:  1 * time.Hour, // Must be positive, doesn't matter for this test.
	}
}

func newSampleGenerator(t *testing.T, initTime time.Time, step time.Duration) <-chan client.Sample {
	ts := make(chan client.Sample)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func(ctx context.Context) {
		c := initTime
		for {
			select {
			case ts <- client.Sample{Value: 0, TimestampMs: util.TimeToMillis(c)}:
			case <-ctx.Done():
				return
			}

			c = c.Add(step)
		}
	}(ctx)

	return ts
}

func TestFlushReasonString(t *testing.T) {
	for fr := flushReason(0); fr < maxFlushReason; fr++ {
		require.True(t, len(fr.String()) > 0)
	}
}
