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

func (m *mockRuleStore) ListRuleGroups(ctx context.Context, userID, namespace string) (rules.RuleGroupList, error) {
	userRules, exists := m.rules[userID]
	if !exists {
		return nil, rules.ErrUserNotFound
	}

	if namespace == "" {
		return userRules, nil
	}

	namespaceRules := rules.RuleGroupList{}

	for _, rg := range userRules {
		if rg.Namespace == namespace {
			namespaceRules = append(namespaceRules, rg)
		}
	}

	if len(namespaceRules) == 0 {
		return nil, rules.ErrGroupNamespaceNotFound
	}

	return namespaceRules, nil
}

func (m *mockRuleStore) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (*rules.RuleGroupDesc, error) {
	userRules, exists := m.rules[userID]
	if !exists {
		return nil, rules.ErrUserNotFound
	}

	if namespace == "" {
		return nil, rules.ErrGroupNamespaceNotFound
	}

	for _, rg := range userRules {
		if rg.Namespace == namespace && rg.Name == group {
			return rg, nil
		}
	}

	return nil, rules.ErrGroupNotFound
}

func (m *mockRuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group *rules.RuleGroupDesc) error {
	userRules, exists := m.rules[userID]
	if !exists {
		userRules = rules.RuleGroupList{}
		m.rules[userID] = userRules
	}

	if namespace == "" {
		return rules.ErrGroupNamespaceNotFound
	}

	for i, rg := range userRules {
		if rg.Namespace == namespace && rg.Name == group.Name {
			userRules[i] = group
			return nil
		}
	}

	m.rules[userID] = append(userRules, group)
	return nil
}

func (m *mockRuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	userRules, exists := m.rules[userID]
	if !exists {
		userRules = rules.RuleGroupList{}
		m.rules[userID] = userRules
	}

	if namespace == "" {
		return rules.ErrGroupNamespaceNotFound
	}

	for i, rg := range userRules {
		if rg.Namespace == namespace && rg.Name == group {
			m.rules[userID] = append(userRules[:i], userRules[:i+1]...)
			return nil
		}
	}

	return nil
}
