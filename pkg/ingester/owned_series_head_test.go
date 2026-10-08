package ingester

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// TestIngester_OwnedSeriesFollowsHeadThroughCompaction drives a real TSDB head
// compaction and asserts that, afterwards, the active series tracker holds exactly
// the series still in the head.
//
// Sample timestamps are explicit, so the head truncates at a known point, while the
// tracker's own timestamps are wall-clock arrival times. A purge keyed on comparing
// the two cannot get this right: entries for series idle shortly before the
// truncation point survive, and owned over-counts until the next compaction. The
// head's PostDeletion callback says exactly which series left, so it can.
func TestIngester_OwnedSeriesFollowsHeadThroughCompaction(t *testing.T) {
	const blockRange = 2 * time.Hour
	ms := func(d time.Duration) int64 { return d.Milliseconds() }

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{blockRange}
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.ActiveSeriesMetricsEnabled = true
	cfg.OwnedSeriesMetricsEnabled = true

	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), i) })
	test.Poll(t, time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })

	ctx := user.InjectOrgID(context.Background(), userID)
	push := func(name string, at ...time.Duration) {
		for _, ts := range at {
			req, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, name), 1, ms(ts))
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// The head spans [0, 3h30m], more than 1.5 block ranges, so compaction cuts the
	// [0, 2h) block and truncates the head at 2h.
	// Pushed in time order, as the head only accepts samples near its max time.
	push("live", 0)
	push("idle_long_ago", 0)
	push("idle_just_before_truncation", 0)
	push("live", time.Hour)
	push("idle_long_ago", time.Hour)
	push("idle_just_before_truncation", 2*time.Hour-5*time.Minute)
	push("live", 2*time.Hour, 3*time.Hour, 3*time.Hour+30*time.Minute)

	db, err := i.getTSDB(userID)
	require.NoError(t, err)
	require.Equal(t, uint64(3), db.Head().NumSeries())
	require.Equal(t, 3, db.activeSeries.Tracked())

	i.compactBlocks(context.Background(), false, nil)

	require.Equal(t, uint64(1), db.Head().NumSeries(), "compaction should leave only the live series in the head")
	require.Equal(t, int(db.Head().NumSeries()), db.activeSeries.Tracked(),
		"the tracker must hold exactly the series still in the head")
	require.Equal(t, int(db.Head().NumSeries()), db.activeSeries.Owned(),
		"owned must follow the head through compaction, with no stale entries")

	// The surviving series keeps receiving samples and stays tracked once.
	push("live", 4*time.Hour)
	require.Equal(t, 1, db.activeSeries.Tracked())
	require.Equal(t, 1, db.activeSeries.Owned())
}
