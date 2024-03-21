package ruler

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/util"
)

func TestBackUpRuleGroups(t *testing.T) {
	r := rulespb.RuleDesc{
		Expr:   "1 > bool 1",
		Record: "test",
	}
	parsedExpr, _ := parser.ParseExpr(r.Expr)
	g1 := rulespb.RuleGroupDesc{
		Name:      "g1",
		Namespace: "ns1",
		Rules:     []*rulespb.RuleDesc{&r},
	}
	g2 := rulespb.RuleGroupDesc{
		Name:      "g2",
		Namespace: "ns1",
		Rules:     []*rulespb.RuleDesc{&r},
	}
	g3 := rulespb.RuleGroupDesc{
		Name:      "g3",
		Namespace: "ns2",
		Rules:     []*rulespb.RuleDesc{&r},
	}

	rInvalid := rulespb.RuleDesc{
		Expr: "1 > 1", //  invalid expression
	}
	gInvalid := rulespb.RuleGroupDesc{
		Name:      "g1",
		Namespace: "ns1",
		Rules:     []*rulespb.RuleDesc{&rInvalid},
	}
	cfg := defaultRulerConfig(t)
	managerOptions := &promRules.ManagerOptions{}
	manager := newRulesBackupManager(cfg, log.NewNopLogger(), nil)
	g1Option := promRules.GroupOptions{
		Name:     g1.Name,
		File:     g1.Namespace,
		Interval: cfg.EvaluationInterval,
		Rules: []promRules.Rule{
			promRules.NewRecordingRule(r.Record, parsedExpr, labels.Labels{}),
		},
	}
	g2Option := promRules.GroupOptions{
		Name:     g2.Name,
		File:     g2.Namespace,
		Interval: cfg.EvaluationInterval,
		Rules: []promRules.Rule{
			promRules.NewRecordingRule(r.Record, parsedExpr, labels.Labels{}),
		},
	}
	g3Option := promRules.GroupOptions{
		Name:     g3.Name,
		File:     g3.Namespace,
		Interval: cfg.EvaluationInterval,
		Rules: []promRules.Rule{
			promRules.NewRecordingRule(r.Record, parsedExpr, labels.Labels{}),
		},
	}

	type testCase struct {
		input          map[string]rulespb.RuleGroupList
		expectedOutput map[string][]*promRules.GroupOptions
	}

	testCases := map[string]testCase{
		"Empty input": {
			input:          make(map[string]rulespb.RuleGroupList),
			expectedOutput: make(map[string][]*promRules.GroupOptions),
		},
		"With invalid rules": {
			input: map[string]rulespb.RuleGroupList{
				"user1": {&gInvalid},
			},
			expectedOutput: make(map[string][]*promRules.GroupOptions),
		},
		"With partial invalid rules": {
			input: map[string]rulespb.RuleGroupList{
				"user1": {&gInvalid, &g3},
				"user2": {&g1, &g2},
			},
			expectedOutput: map[string][]*promRules.GroupOptions{
				"user2": {&g1Option, &g2Option},
			},
		},
		"With groups from multiple users": {
			input: map[string]rulespb.RuleGroupList{
				"user1": {&g1, &g2, &g3},
				"user2": {&g1, &g2},
			},
			expectedOutput: map[string][]*promRules.GroupOptions{
				"user1": {&g1Option, &g2Option, &g3Option},
				"user2": {&g1Option, &g2Option},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			manager.backUpRuleGroups(context.TODO(), tc.input)
			require.Equal(t, len(tc.expectedOutput), len(manager.inMemoryRuleGroupsBackup))
			for user, expectedGroupOptions := range tc.expectedOutput {
				loadedGroups := manager.getRuleGroups(user)
				expectedGroups := make([]*promRules.Group, 0, len(expectedGroupOptions))
				for _, o := range expectedGroupOptions {
					o.Opts = managerOptions
					expectedGroups = append(expectedGroups, promRules.NewGroup(*o))
				}
				requireGroupsEqual(t, expectedGroups, loadedGroups)
			}
		})
	}
}

func TestBackUpRuleGroupsMetrics(t *testing.T) {
	r := rulespb.RuleDesc{
		Expr:   "1 > bool 1",
		Record: "test",
	}
	g1 := rulespb.RuleGroupDesc{
		Name:      "g1",
		Namespace: "ns1",
		Rules:     []*rulespb.RuleDesc{&r},
	}
	g2 := rulespb.RuleGroupDesc{
		Name:      "g2",
		Namespace: "ns1",
		Rules:     []*rulespb.RuleDesc{&r},
	}
	g2Updated := rulespb.RuleGroupDesc{
		Name:      "g2",
		Namespace: "ns1",
		Rules:     []*rulespb.RuleDesc{&r, &r},
	}

	cfg := defaultRulerConfig(t)
	reg := prometheus.NewPedanticRegistry()
	manager := newRulesBackupManager(cfg, log.NewNopLogger(), reg)

	manager.backUpRuleGroups(context.TODO(), map[string]rulespb.RuleGroupList{
		"user1": {&g1, &g2},
		"user2": {&g1},
	})
	gm, err := reg.Gather()
	require.NoError(t, err)
	mfm, err := util.NewMetricFamilyMap(gm)
	require.NoError(t, err)
	require.Equal(t, 2, len(mfm["cortex_ruler_backup_last_reload_successful"].Metric))
	requireMetricEqual(t, mfm["cortex_ruler_backup_last_reload_successful"].Metric[0], map[string]string{
		"user": "user1",
	}, float64(1))
	requireMetricEqual(t, mfm["cortex_ruler_backup_last_reload_successful"].Metric[1], map[string]string{
		"user": "user2",
	}, float64(1))
	require.Equal(t, 3, len(mfm["cortex_ruler_backup_rule_group_rules"].Metric))
	requireMetricEqual(t, mfm["cortex_ruler_backup_rule_group_rules"].Metric[0], map[string]string{
		"user":       "user1",
		"rule_group": "ns1;g1",
	}, float64(1))
	requireMetricEqual(t, mfm["cortex_ruler_backup_rule_group_rules"].Metric[1], map[string]string{
		"user":       "user2",
		"rule_group": "ns1;g1",
	}, float64(1))
	requireMetricEqual(t, mfm["cortex_ruler_backup_rule_group_rules"].Metric[2], map[string]string{
		"user":       "user1",
		"rule_group": "ns1;g2",
	}, float64(1))

	manager.backUpRuleGroups(context.TODO(), map[string]rulespb.RuleGroupList{
		"user1": {&g2Updated},
	})
	gm, err = reg.Gather()
	require.NoError(t, err)
	mfm, err = util.NewMetricFamilyMap(gm)
	require.NoError(t, err)
	require.Equal(t, 1, len(mfm["cortex_ruler_backup_last_reload_successful"].Metric))
	requireMetricEqual(t, mfm["cortex_ruler_backup_last_reload_successful"].Metric[0], map[string]string{
		"user": "user1",
	}, float64(1))
	require.Equal(t, 1, len(mfm["cortex_ruler_backup_rule_group_rules"].Metric))
	requireMetricEqual(t, mfm["cortex_ruler_backup_rule_group_rules"].Metric[0], map[string]string{
		"user":       "user1",
		"rule_group": "ns1;g2",
	}, float64(2))
}

func requireGroupsEqual(t *testing.T, a []*promRules.Group, b []*promRules.Group) {
	require.Equal(t, len(a), len(b))
	sortFunc := func(g1, g2 *promRules.Group) int {
		fileCompare := strings.Compare(g1.File(), g2.File())
		if fileCompare != 0 {
			return fileCompare
		}
		return strings.Compare(g1.Name(), g2.Name())
	}
	slices.SortFunc(a, sortFunc)
	slices.SortFunc(b, sortFunc)
	for i, gA := range a {
		gB := b[i]
		require.True(t, gA.Equals(gB), "group1", gA.Name(), "group2", gB.Name())
	}
}

func requireMetricEqual(t *testing.T, m *io_prometheus_client.Metric, labels map[string]string, value float64) {
	l := m.GetLabel()
	require.Equal(t, len(labels), len(l))
	for _, pair := range l {
		require.Equal(t, labels[*pair.Name], *pair.Value)
	}
	require.Equal(t, value, *m.Gauge.Value)
}
