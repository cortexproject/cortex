package ruler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/notifier"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestSyncRuleGroups(t *testing.T) {
	dir := t.TempDir()

	waitDurations := []time.Duration{
		1 * time.Millisecond,
		1 * time.Millisecond,
	}

	ruleManagerFactory := RuleManagerFactory(nil, waitDurations)

	m, err := NewDefaultMultiTenantManager(Config{RulePath: dir}, ruleManagerFactory, nil, nil, log.NewNopLogger())
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

func TestSlowRuleGroupSyncDoesNotSlowdownListRules(t *testing.T) {
	dir := t.TempDir()
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

	groupsToReturn := [][]*promRules.Group{
		{
			promRules.NewGroup(promRules.GroupOptions{
				Name:     "group1",
				File:     "ns",
				Interval: 60,
				Limit:    0,
				Opts:     &promRules.ManagerOptions{},
			}),
		},
		{
			promRules.NewGroup(promRules.GroupOptions{
				Name:     "group1",
				File:     "ns",
				Interval: 60,
				Limit:    0,
				Opts:     &promRules.ManagerOptions{},
			}),
			promRules.NewGroup(promRules.GroupOptions{
				Name:     "group2",
				File:     "ns",
				Interval: 60,
				Limit:    0,
				Opts:     &promRules.ManagerOptions{},
			}),
		},
	}

	waitDurations := []time.Duration{
		5 * time.Millisecond,
		1 * time.Second,
	}

	ruleManagerFactory := RuleManagerFactory(groupsToReturn, waitDurations)
	m, err := NewDefaultMultiTenantManager(Config{RulePath: dir}, ruleManagerFactory, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)

	m.SyncRuleGroups(context.Background(), userRules)
	mgr := getManager(m, user)
	require.NotNil(t, mgr)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		return mgr.(*mockRulesManager).running.Load()
	})
	groups := m.GetRules(user)
	require.Len(t, groups, len(groupsToReturn[0]), "expected %d but got %d", len(groupsToReturn[0]), len(groups))

	// update rules and call list rules concurrently
	userRules = map[string]rulespb.RuleGroupList{
		user: {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "ns",
				Interval:  1 * time.Minute,
				User:      user,
			},
			&rulespb.RuleGroupDesc{
				Name:      "group2",
				Namespace: "ns",
				Interval:  1 * time.Minute,
				User:      user,
			},
		},
	}
	go m.SyncRuleGroups(context.Background(), userRules)

	groups = m.GetRules(user)

	require.Len(t, groups, len(groupsToReturn[0]), "expected %d but got %d", len(groupsToReturn[0]), len(groups))
	test.Poll(t, 5*time.Second, len(groupsToReturn[1]), func() interface{} {
		groups = m.GetRules(user)
		return len(groups)
	})

	test.Poll(t, 1*time.Second, true, func() interface{} {
		return mgr.(*mockRulesManager).running.Load()
	})

	m.Stop()

	test.Poll(t, 1*time.Second, false, func() interface{} {
		return mgr.(*mockRulesManager).running.Load()
	})
}

func TestSyncRuleGroupsCleanUpPerUserMetrics(t *testing.T) {
	dir := t.TempDir()
	reg := prometheus.NewPedanticRegistry()
	evalMetrics := NewRuleEvalMetrics(Config{RulePath: dir, EnableQueryStats: true}, reg)

	waitDurations := []time.Duration{
		1 * time.Millisecond,
		1 * time.Millisecond,
	}

	ruleManagerFactory := RuleManagerFactory(nil, waitDurations)

	m, err := NewDefaultMultiTenantManager(Config{RulePath: dir}, ruleManagerFactory, evalMetrics, reg, log.NewNopLogger())
	require.NoError(t, err)

	const user = "testUser"

	evalMetrics.TotalWritesVec.WithLabelValues(user).Add(10)

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
	gm, err := reg.Gather()
	require.NoError(t, err)
	mfm, err := util.NewMetricFamilyMap(gm)
	require.NoError(t, err)
	require.Contains(t, mfm["cortex_ruler_write_requests_total"].String(), "value:\""+user+"\"")
	require.Contains(t, mfm["cortex_ruler_config_last_reload_successful"].String(), "value:\""+user+"\"")

	// Passing empty map / nil stops all managers.
	m.SyncRuleGroups(context.Background(), nil)
	require.Nil(t, getManager(m, user))

	gm, err = reg.Gather()
	require.NoError(t, err)
	mfm, err = util.NewMetricFamilyMap(gm)
	require.NoError(t, err)
	require.NotContains(t, mfm["cortex_ruler_write_requests_total"].String(), "value:\""+user+"\"")
	require.NotContains(t, mfm["cortex_ruler_config_last_reload_successful"].String(), "value:\""+user+"\"")
}

func TestBackupRules(t *testing.T) {
	dir := t.TempDir()
	reg := prometheus.NewPedanticRegistry()
	evalMetrics := NewRuleEvalMetrics(Config{RulePath: dir, EnableQueryStats: true}, reg)
	waitDurations := []time.Duration{
		1 * time.Millisecond,
		1 * time.Millisecond,
	}
	ruleManagerFactory := RuleManagerFactory(nil, waitDurations)
	config := Config{RulePath: dir}
	config.Ring.ReplicationFactor = 3
	m, err := NewDefaultMultiTenantManager(config, ruleManagerFactory, evalMetrics, reg, log.NewNopLogger())
	require.NoError(t, err)

	const user1 = "testUser"
	const user2 = "testUser2"

	require.Equal(t, 0, len(m.GetBackupRules(user1)))
	require.Equal(t, 0, len(m.GetBackupRules(user2)))

	userRules := map[string]rulespb.RuleGroupList{
		user1: {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "ns",
				Interval:  1 * time.Minute,
				User:      user1,
			},
		},
		user2: {
			&rulespb.RuleGroupDesc{
				Name:      "group2",
				Namespace: "ns",
				Interval:  1 * time.Minute,
				User:      user1,
			},
		},
	}
	m.BackUpRuleGroups(context.TODO(), userRules)
	require.Equal(t, userRules[user1], m.GetBackupRules(user1))
	require.Equal(t, userRules[user2], m.GetBackupRules(user2))
}

func getManager(m *DefaultMultiTenantManager, user string) RulesManager {
	m.userManagerMtx.RLock()
	defer m.userManagerMtx.RUnlock()

	return m.userManagers[user]
}

func RuleManagerFactory(groupsToReturn [][]*promRules.Group, waitDurations []time.Duration) ManagerFactory {
	return func(_ context.Context, _ string, _ *notifier.Manager, _ log.Logger, _ prometheus.Registerer) RulesManager {
		return &mockRulesManager{
			done:           make(chan struct{}),
			groupsToReturn: groupsToReturn,
			waitDurations:  waitDurations,
			iteration:      -1,
		}
	}
}

type mockRulesManager struct {
	mtx            sync.Mutex
	groupsToReturn [][]*promRules.Group
	iteration      int
	waitDurations  []time.Duration
	running        atomic.Bool
	done           chan struct{}
}

func (m *mockRulesManager) Update(_ time.Duration, _ []string, _ labels.Labels, _ string, _ promRules.GroupEvalIterationFunc) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	ticker := time.NewTicker(m.waitDurations[m.iteration+1])
	select {
	case <-ticker.C:
		m.iteration = m.iteration + 1
		return nil
	case <-m.done:
		return nil
	}
}

func (m *mockRulesManager) RuleGroups() []*promRules.Group {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.iteration < 0 {
		return nil
	}
	return m.groupsToReturn[m.iteration]
}

func (m *mockRulesManager) Run() {
	m.running.Store(true)
	<-m.done
}

func (m *mockRulesManager) Stop() {
	m.running.Store(false)
	close(m.done)
}
