package ruler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/notifier"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestSyncRuleGroups(t *testing.T) {
	dir := t.TempDir()

	m, err := NewDefaultMultiTenantManager(Config{RulePath: dir}, factory, nil, log.NewNopLogger())
	require.NoError(t, err)

	const user = "testUser"

	userRules := map[string]rulespb.RuleGroupList{
		user: {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "ns",
				Interval:  1 * time.Minute,
				User:      user,
			},
		},
	}
	m.SyncRuleGroups(context.Background(), userRules)

	mgr := getManager(m, user)
	require.NotNil(t, mgr)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		return mgr.(*mockRulesManager).running.Load()
	})

	// Verify that user rule groups are now cached locally and notifiers are created.
	{
		users, err := m.mapper.users()
		_, ok := m.notifiers[user]
		require.NoError(t, err)
		require.Equal(t, []string{user}, users)
		require.True(t, ok)
	}

	// Passing empty map / nil stops all managers.
	m.SyncRuleGroups(context.Background(), nil)
	require.Nil(t, getManager(m, user))

	// Make sure old manager was stopped.
	test.Poll(t, 1*time.Second, false, func() interface{} {
		return mgr.(*mockRulesManager).running.Load()
	})

	// Verify that local rule groups were removed.
	{
		users, err := m.mapper.users()
		_, ok := m.notifiers[user]
		require.NoError(t, err)
		require.Equal(t, []string(nil), users)
		require.False(t, ok)
	}

	// Resync same rules as before. Previously this didn't restart the manager.
	m.SyncRuleGroups(context.Background(), userRules)

	newMgr := getManager(m, user)
	require.NotNil(t, newMgr)
	require.True(t, mgr != newMgr)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		return newMgr.(*mockRulesManager).running.Load()
	})

	// Verify that user rule groups are cached locally again.
	{
		users, err := m.mapper.users()
		require.NoError(t, err)
		require.Equal(t, []string{user}, users)
	}

	m.Stop()

	test.Poll(t, 1*time.Second, false, func() interface{} {
		return newMgr.(*mockRulesManager).running.Load()
	})
}

func TestForStateSync(t *testing.T) {
	suite, err := promql.NewTest(t, `
		load 1m
			metric{} 1
			metric{} 1
	`)
	require.NoError(t, err)
	defer suite.Close()

	require.NoError(t, suite.Run())

	expr, err := parser.ParseExpr(`metric > 0`)
	require.NoError(t, err)

	type testInput struct {
		alertForStateValue float64
		expectedActiveAt   time.Time
	}

	tests := []testInput{
		// activeAt should NOT be updated to epoch time 0 since (activeAt < alertForStateValue)
		{
			alertForStateValue: 300,
			expectedActiveAt:   time.Unix(0, 0),
		},
		// activeAt should be updated to epoch time 0 since (alertForStateValue < activeAt)
		{
			alertForStateValue: 0,
			expectedActiveAt:   time.Unix(0, 0),
		},
		// activeAt should be epoch time 0 since (alertForStateValue = activeAt = 0)
		{
			alertForStateValue: 0,
			expectedActiveAt:   time.Unix(0, 0),
		},
	}

	testFunc := func(tst testInput) {
		labelSet := []string{"__name__", "ALERTS_FOR_STATE", "alertname", "HTTPRequestRateLow"}

		selectMockFunction := func(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
			return storage.TestSeriesSet(storage.MockSeries([]int64{0, 1}, []float64{tst.alertForStateValue, tst.alertForStateValue}, labelSet))
		}
		mockQueryable := storage.MockQueryable{
			MockQuerier: &storage.MockQuerier{
				SelectMockFunction: selectMockFunction,
			},
		}

		opts := &promRules.ManagerOptions{
			Queryable:       &mockQueryable,
			Context:         context.Background(),
			Logger:          log.NewNopLogger(),
			NotifyFunc:      func(ctx context.Context, expr string, alerts ...*promRules.Alert) {},
			OutageTolerance: 30 * time.Minute,
			ForGracePeriod:  10 * time.Minute,
		}

		rule := promRules.NewAlertingRule(
			"HTTPRequestRateLow",
			expr,
			25*time.Minute,
			nil,
			nil, nil, "", true, nil,
		)
		evalTime := time.Unix(0, 0)
		_, err := rule.Eval(suite.Context(), evalTime, promRules.EngineQueryFunc(suite.QueryEngine(), suite.Storage()), nil, 0)
		require.NoError(t, err)

		group := promRules.NewGroup(promRules.GroupOptions{
			Name:          "default",
			Interval:      time.Second,
			Rules:         []promRules.Rule{rule},
			ShouldRestore: true,
			Opts:          opts,
		})

		err = syncAlertsActiveAt(group, evalTime, log.NewNopLogger())
		require.NoError(t, err)
		require.Equal(t, tst.expectedActiveAt.UTC(), rule.ActiveAlerts()[0].ActiveAt.UTC())
	}

	for _, tst := range tests {
		testFunc(tst)
	}
}

func getManager(m *DefaultMultiTenantManager, user string) RulesManager {
	m.userManagerMtx.Lock()
	defer m.userManagerMtx.Unlock()

	return m.userManagers[user]
}

func factory(_ context.Context, _ string, _ *notifier.Manager, _ log.Logger, _ prometheus.Registerer) RulesManager {
	return &mockRulesManager{done: make(chan struct{})}
}

type mockRulesManager struct {
	running atomic.Bool
	done    chan struct{}
}

func (m *mockRulesManager) Run() {
	m.running.Store(true)
	<-m.done
}

func (m *mockRulesManager) Stop() {
	m.running.Store(false)
	close(m.done)
}

func (m *mockRulesManager) Update(_ time.Duration, _ []string, _ labels.Labels, _ string, _ promRules.RuleGroupPostProcessFunc) error {
	return nil
}

func (m *mockRulesManager) RuleGroups() []*promRules.Group {
	return nil
}
