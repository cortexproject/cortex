package ruler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/assert"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/notifier"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ha"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
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

func TestEvalIterationFunc(t *testing.T) {
	type testcase struct {
		name               string
		haTrackerEnabled   bool
		electedReplica     string
		evaluationExpected bool
	}

	testcases := []testcase{
		{
			name:               "no-ha-tracker",
			haTrackerEnabled:   false,
			electedReplica:     "dummy",
			evaluationExpected: true,
		},
		{
			name:               "ha-tracker-non-leader",
			haTrackerEnabled:   true,
			electedReplica:     "ruler-another",
			evaluationExpected: false,
		},
		{
			name:               "ha-tracker-leader",
			haTrackerEnabled:   true,
			electedReplica:     "ruler-test",
			evaluationExpected: true,
		},
	}

	ctx := context.Background()
	now := time.Now()

	for i, tc := range testcases {
		t.Run(fmt.Sprintf("%d: %s", i, tc.name), func(t *testing.T) {
			kvStore, cleanUp := consul.NewInMemoryClient(ha.GetReplicaDescCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, cleanUp.Close()) })

			err := kvStore.Put(ctx, "testUser/testFile/testGroup", &ha.ReplicaDesc{
				ReceivedAt: timestamp.FromTime(now) - 60,
				Replica:    tc.electedReplica,
				DeletedAt:  0,
			})
			require.NoError(t, err)

			cfg := Config{
				PollInterval: 5 * time.Second,
				HATrackerConfig: HATrackerConfig{
					EnableHATracker:        tc.haTrackerEnabled,
					UpdateTimeout:          300 * time.Second,
					UpdateTimeoutJitterMax: 0,
					FailoverTimeout:        300 * time.Second,
					KVStore: kv.Config{
						Mock: kvStore,
					},
					ReplicaID: "ruler-test",
				},
			}

			manager := newManager(t, cfg)

			user := "testUser"

			testRule := &mockRule{name: "testRule"}

			g := promRules.NewGroup(promRules.GroupOptions{
				Name:     "testGroup",
				File:     "testFile",
				Interval: time.Second,
				Rules:    []promRules.Rule{testRule},
				Opts: &promRules.ManagerOptions{
					Appendable: NewPusherAppendable(&fakePusher{}, user, ruleLimits{}, prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewCounter(prometheus.CounterOpts{})),
					Logger:     log.NewNopLogger(),
				},
			})

			require.True(t, g.GetLastEvalTimestamp().IsZero())

			// first iteration will be skipped for ha-tracker-leader case
			manager.evalIterationFunc(ctx, user, g, now)
			time.Sleep(time.Second * 5)
			manager.evalIterationFunc(ctx, user, g, now)

			if tc.evaluationExpected {
				require.Equal(t, now, g.GetLastEvalTimestamp())
			} else {
				require.True(t, g.GetLastEvalTimestamp().IsZero())
			}
		})
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

func (m *mockRulesManager) Update(_ time.Duration, _ []string, _ labels.Labels, _ string, _ promRules.GroupEvalIterationFunc) error {
	return nil
}

func (m *mockRulesManager) RuleGroups() []*promRules.Group {
	return nil
}
