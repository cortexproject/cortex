package ruler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
)

type mockRuleStore struct {
	rules map[string]rulestore.RuleGroupList
	mtx   sync.Mutex
}

var (
	interval, _         = time.ParseDuration("1m")
	mockRulesNamespaces = map[string]rulestore.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
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
			&rulespb.RuleGroupDesc{
				Name:      "fail",
				Namespace: "namespace2",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Record: "UP2_RULE",
						Expr:   "up",
					},
					{
						Alert: "UP2_ALERT",
						Expr:  "up < 1",
					},
				},
				Interval: interval,
			},
		},
	}
	mockRules = map[string]rulestore.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
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
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user2",
				Rules: []*rulespb.RuleDesc{
					{
						Record: "UP_RULE",
						Expr:   "up",
					},
				},
				Interval: interval,
			},
		},
	}

	mockSpecialCharRules = map[string]rulestore.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      ")(_+?/|group1+/?",
				Namespace: ")(_+?/|namespace1+/?",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
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
	}
)

func newMockRuleStore(rules map[string]rulestore.RuleGroupList) *mockRuleStore {
	return &mockRuleStore{
		rules: rules,
	}
}

func (m *mockRuleStore) SupportsModifications() bool {
	return true
}

func (m *mockRuleStore) ListAllUsers(_ context.Context) ([]string, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	var result []string
	for u := range m.rules {
		result = append(result, u)
	}
	return result, nil
}

func (m *mockRuleStore) ListAllRuleGroups(_ context.Context) (map[string]rulestore.RuleGroupList, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	result := make(map[string]rulestore.RuleGroupList)
	for k, v := range m.rules {
		result[k] = append(rulestore.RuleGroupList(nil), v...)
	}

	return result, nil
}

func (m *mockRuleStore) ListRuleGroupsForUserAndNamespace(_ context.Context, userID, namespace string) (rulestore.RuleGroupList, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	userRules, exists := m.rules[userID]
	if !exists {
		return rulestore.RuleGroupList{}, nil
	}

	if namespace == "" {
		return userRules, nil
	}

	namespaceRules := rulestore.RuleGroupList{}

	for _, rg := range userRules {
		if rg.Namespace == namespace {
			namespaceRules = append(namespaceRules, rg)
		}
	}

	if len(namespaceRules) == 0 {
		return rulestore.RuleGroupList{}, nil
	}

	return namespaceRules, nil
}

func (m *mockRuleStore) LoadRuleGroups(ctx context.Context, groupsToLoad map[string]rulestore.RuleGroupList) error {
	// Nothing to do, as mockRuleStore already returns groups with loaded rules.
	return nil
}

func (m *mockRuleStore) GetRuleGroup(_ context.Context, userID string, namespace string, group string) (*rulespb.RuleGroupDesc, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	userRules, exists := m.rules[userID]
	if !exists {
		return nil, rulestore.ErrUserNotFound
	}

	if namespace == "" {
		return nil, rulestore.ErrGroupNamespaceNotFound
	}

	for _, rg := range userRules {
		if rg.Namespace == namespace && rg.Name == group {
			return rg, nil
		}
	}

	return nil, rulestore.ErrGroupNotFound
}

func (m *mockRuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group *rulespb.RuleGroupDesc) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	userRules, exists := m.rules[userID]
	if !exists {
		userRules = rulestore.RuleGroupList{}
		m.rules[userID] = userRules
	}

	if namespace == "" {
		return rulestore.ErrGroupNamespaceNotFound
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
	m.mtx.Lock()
	defer m.mtx.Unlock()

	userRules, exists := m.rules[userID]
	if !exists {
		userRules = rulestore.RuleGroupList{}
		m.rules[userID] = userRules
	}

	if namespace == "" {
		return rulestore.ErrGroupNamespaceNotFound
	}

	for i, rg := range userRules {
		if rg.Namespace == namespace && rg.Name == group {
			m.rules[userID] = append(userRules[:i], userRules[:i+1]...)
			return nil
		}
	}

	return nil
}

func (m *mockRuleStore) DeleteNamespace(ctx context.Context, userID, namespace string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	userRules, exists := m.rules[userID]
	if !exists {
		userRules = rulestore.RuleGroupList{}
		m.rules[userID] = userRules
	}

	if namespace == "" {
		return rulestore.ErrGroupNamespaceNotFound
	}

	for i, rg := range userRules {
		if rg.Namespace == namespace {

			// Only here to assert on partial failures.
			if rg.Name == "fail" {
				return fmt.Errorf("unable to delete rg")
			}

			m.rules[userID] = append(userRules[:i], userRules[i+1:]...)
		}
	}

	return nil
}
