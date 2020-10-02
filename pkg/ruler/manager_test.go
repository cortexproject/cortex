package ruler

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/pkg/labels"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestSyncRuleGroups(t *testing.T) {
	dir, err := ioutil.TempDir("", "rules")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	m, err := NewDefaultMultiTenantManager(Config{RulePath: dir}, factory, nil, log.NewNopLogger())
	require.NoError(t, err)

	const user = "testUser"

	userRules := map[string]rules.RuleGroupList{
		user: {
			&rules.RuleGroupDesc{
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

	// Passing empty map / nil stops all managers.
	m.SyncRuleGroups(context.Background(), nil)
	require.Nil(t, getManager(m, user))

	// Make sure old manager was stopped.
	test.Poll(t, 1*time.Second, false, func() interface{} {
		return mgr.(*mockRulesManager).running.Load()
	})

	// Resync same rules as before. Previously this didn't restart the manager.
	m.SyncRuleGroups(context.Background(), userRules)

	newMgr := getManager(m, user)
	require.NotNil(t, newMgr)
	require.True(t, mgr != newMgr)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		return newMgr.(*mockRulesManager).running.Load()
	})

	m.Stop()

	test.Poll(t, 1*time.Second, false, func() interface{} {
		return newMgr.(*mockRulesManager).running.Load()
	})
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

func (m *mockRulesManager) Update(_ time.Duration, _ []string, _ labels.Labels) error {
	return nil
}

func (m *mockRulesManager) RuleGroups() []*promRules.Group {
	return nil
}
