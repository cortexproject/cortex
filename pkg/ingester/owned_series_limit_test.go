package ingester

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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

const ownedLimitTestLocalLimit = 100

// prepareIngesterHoldingUnownedSeries reproduces the state an ingester is in right
// after a scale-up: its head holds more series than its local limit, but it owns
// none of them any more, because the ring moved them to other ingesters.
//
// The ring is faked by installing ownership state in which this ingester owns no
// token. The periodic metrics update, which would overwrite it with the real ring,
// is pushed out of the test's reach.
func prepareIngesterHoldingUnownedSeries(t *testing.T) (*Ingester, *userTSDB, func(name string) error) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.ActiveSeriesMetricsEnabled = true
	cfg.ActiveSeriesMetricsUpdatePeriod = time.Hour
	cfg.OwnedSeriesMetricsEnabled = true
	cfg.OwnedSeriesLimitEnforcementEnabled = true

	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerUser = ownedLimitTestLocalLimit

	i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), i) })
	test.Poll(t, time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })

	ctx := user.InjectOrgID(context.Background(), userID)
	nowMs := time.Now().UnixMilli()
	push := func(name string) error {
		req, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, name), 1, nowMs)
		_, err := i.Push(ctx, req)
		return err
	}

	require.NoError(t, push("warmup"))
	db, err := i.getTSDB(userID)
	require.NoError(t, err)

	// From here on this ingester owns nothing: existing entries are re-evaluated,
	// and new ones are created unowned.
	updateMetricsWithRing(db.activeSeries, time.Now().Add(-time.Hour), []uint32{1})
	require.Equal(t, 0, db.activeSeries.Owned())

	// Fill the head past the local limit. Each push is accepted because owned
	// stays at 0.
	for n := range 150 {
		require.NoError(t, push(fmt.Sprintf("moved_away_%d", n)))
	}
	require.Equal(t, uint64(151), db.Head().NumSeries())
	require.Equal(t, 151, db.activeSeries.Tracked())
	require.Equal(t, 0, db.activeSeries.Owned())
	require.Greater(t, int(db.Head().NumSeries()), ownedLimitTestLocalLimit, "head must exceed the local limit for the test to mean anything")

	return i, db, push
}

func isPerUserSeriesLimitErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "per-user series limit")
}

// TestIngester_OwnedSeriesLimit_UntrackedHeadSeriesDoesNotDisableOwned pins the
// window that every new series passes through: it is in the head, but its tracker
// entry has not been written yet. A concurrent push checking the limit in that
// window sees tracked < head.
//
// That must not switch the limit check back to the whole head. Doing so rejects a
// tenant that owns nothing, which is exactly the false throttling the owned count
// exists to prevent.
func TestIngester_OwnedSeriesLimit_UntrackedHeadSeriesDoesNotDisableOwned(t *testing.T) {
	_, db, push := prepareIngesterHoldingUnownedSeries(t)

	// Stand in for a concurrent push that has created its series in the head and
	// not yet recorded it in the tracker.
	app := db.Appender(context.Background())
	_, err := app.Append(0, labels.FromStrings(labels.MetricName, "in_flight"), time.Now().UnixMilli(), 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.Equal(t, int(db.Head().NumSeries())-1, db.activeSeries.Tracked(), "one head series must be untracked")

	err = push("after_in_flight")
	require.NoError(t, err, "a single untracked head series must not make the limit count the whole head")
}

// TestIngester_OwnedSeriesLimit_ConcurrentNewSeries drives the same window with real
// concurrent pushes, each creating a new series, on an ingester whose head is over
// the local limit and which owns none of it.
func TestIngester_OwnedSeriesLimit_ConcurrentNewSeries(t *testing.T) {
	_, _, push := prepareIngesterHoldingUnownedSeries(t)

	const (
		workers         = 32
		seriesPerWorker = 20
	)
	var (
		wg       sync.WaitGroup
		rejected atomic.Int64
		other    atomic.Int64
	)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range seriesPerWorker {
				err := push(fmt.Sprintf("concurrent_%d_%d", w, n))
				switch {
				case err == nil:
				case isPerUserSeriesLimitErr(err):
					rejected.Add(1)
				default:
					other.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.Zero(t, other.Load(), "unexpected non-limit push errors")
	require.Zero(t, rejected.Load(), "%d of %d new series were rejected by the per-user limit although the ingester owns none of its head", rejected.Load(), workers*seriesPerWorker)
}
