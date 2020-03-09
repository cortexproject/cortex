package ruler

import (
	"context"
	"time"

	"github.com/cortexproject/cortex/pkg/ruler/rules"
)

type mockRuleStore struct {
	rules map[string]rules.RuleGroupList
}

var (
	interval, _ = time.ParseDuration("1m")
	mockRules   = map[string]rules.RuleGroupList{
		"user1": {
			&rules.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules: []*rules.RuleDesc{
					{
						Record: "UP_RULE",
						Expr:   "up",
					},
					{
						Alert: "UP_ALERT",
						Expr:  "up < 1",
					},
				},
				Interval: interval,
			},
		},
		"user2": {
			&rules.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user2",
				Rules: []*rules.RuleDesc{
					{
						Record: "UP_RULE",
						Expr:   "up",
					},
				},
				Interval: interval,
			},
		},
	}
)

func newMockRuleStore(rules map[string]rules.RuleGroupList) *mockRuleStore {
	return &mockRuleStore{
		rules: rules,
	}
}

func (m *mockRuleStore) ListAllRuleGroups(ctx context.Context) (map[string]rules.RuleGroupList, error) {
	return m.rules, nil
}
