package ingester

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const ownedLimitTestLocalLimit = 100

// prepareIngesterHoldingUnownedSeries reproduces the state an ingester is in right
// after a scale-up: its head holds more series than its local limit, but it owns
// none of them any more, because the ring moved them to other ingesters.
//
// The head is filled while the tenant's limit is still high. The ring change is
// then faked by installing ownership state in which this ingester owns no token,
// and the limit lowered, as the scale-up lowers the local limit. The periodic
// updates, which would overwrite the ownership with the real ring, are pushed out
// of the test's reach.
func prepareIngesterHoldingUnownedSeries(t *testing.T) (*Ingester, *userTSDB, func(name string) error) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.ActiveSeriesMetricsEnabled = true
	cfg.ActiveSeriesMetricsUpdatePeriod = time.Hour
	cfg.ownedSeriesCheckPeriod = time.Hour
	cfg.OwnedSeriesMetricsEnabled = true
	cfg.OwnedSeriesLimitEnforcementEnabled = true

	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerUser = 10 * ownedLimitTestLocalLimit
	tenantLimits := &mutableTenantLimits{limits: map[string]*validation.Limits{}}

	i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, tenantLimits, "", prometheus.NewRegistry())
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

	for n := range 151 {
		require.NoError(t, push(fmt.Sprintf("moved_away_%d", n)))
	}
	db, err := i.getTSDB(userID)
	require.NoError(t, err)

	// The scale-up: this ingester owns none of what it holds, and the local
	// limit drops below what it holds.
	ringTokens := []uint32{1}
	positions := ownedPositionsFor(ringTokens)
	// A view of the ring newer than every series pushed, so they are all judged.
	db.activeSeries.UpdateMetrics(time.Now().Add(-time.Hour), ringTokens, positions, testRingFingerprint(ringTokens, positions), i.lifecycler.RingReads()+1)
	lowered := limits
	lowered.MaxLocalSeriesPerUser = ownedLimitTestLocalLimit
	tenantLimits.mu.Lock()
	tenantLimits.limits[userID] = &lowered
	tenantLimits.mu.Unlock()

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

	// New series count as owned when they arrive, so stay below the local limit:
	// the point is that series in flight must not make the limit count the head.
	const (
		workers         = 32
		seriesPerWorker = 2
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

// TestIngester_OwnedSeriesInstanceLimit_CountsSeriesCreatedSinceSnapshot covers the
// instance max_series limit between two updateActiveSeries cycles. The owned count
// it reads is a snapshot taken once per cycle, so series created after it must be
// added on top. Otherwise the limit, which protects the ingester from running out
// of memory, lets any number of new series through until the next cycle.
func TestIngester_OwnedSeriesInstanceLimit_CountsSeriesCreatedSinceSnapshot(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.ActiveSeriesMetricsEnabled = true
	// Cycles run only when the test calls updateActiveSeries.
	cfg.ActiveSeriesMetricsUpdatePeriod = time.Hour
	cfg.ownedSeriesCheckPeriod = time.Hour
	cfg.OwnedSeriesMetricsEnabled = true
	cfg.OwnedSeriesLimitEnforcementEnabled = true
	cfg.InstanceLimitsFn = func() *InstanceLimits { return &InstanceLimits{MaxInMemorySeries: 2} }

	i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, "", prometheus.NewRegistry())
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

	require.NoError(t, push("first"))
	i.updateActiveSeries(ctx)

	require.NoError(t, push("second"), "one owned series plus one new one is within the limit of 2")
	for _, name := range []string{"third", "fourth"} {
		require.ErrorContains(t, push(name), "max series limit reached", "series %q must be rejected: the ingester already holds 2", name)
	}

	db, err := i.getTSDB(userID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), db.Head().NumSeries())
}

func TestInstanceOwnedSnapshot_Count(t *testing.T) {
	tests := map[string]struct {
		snapshot       instanceOwnedSnapshot
		seriesCreated  int64
		seriesInMemory int64
		expected       int64
	}{
		"nothing created since the snapshot": {
			snapshot:       instanceOwnedSnapshot{owned: 40, seriesCreated: 100},
			seriesCreated:  100,
			seriesInMemory: 100,
			expected:       40,
		},
		"series created since the snapshot are added": {
			snapshot:       instanceOwnedSnapshot{owned: 40, seriesCreated: 100},
			seriesCreated:  130,
			seriesInMemory: 130,
			expected:       70,
		},
		// Head compaction deleted 30 series that had moved to other ingesters, and 30
		// new ones were created. The in-memory count is unchanged, but the 30 new
		// series still have to be counted.
		"series created while as many were deleted are still counted": {
			snapshot:       instanceOwnedSnapshot{owned: 40, seriesCreated: 100},
			seriesCreated:  130,
			seriesInMemory: 100,
			expected:       70,
		},
		"never more than the series in memory": {
			snapshot:       instanceOwnedSnapshot{owned: 90, seriesCreated: 100},
			seriesCreated:  130,
			seriesInMemory: 100,
			expected:       100,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.snapshot.count(tc.seriesCreated, tc.seriesInMemory))
		})
	}
}
