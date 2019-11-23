package ruler

import (
	"context"
	"strings"
	"sync"

	"github.com/cortexproject/cortex/pkg/ruler/rules"
)

type mockRuleStore struct {
	sync.Mutex
	rules map[string]*rules.RuleGroupDesc
}

func newMockRuleStore() *mockRuleStore {
	return &mockRuleStore{
		rules: map[string]*rules.RuleGroupDesc{
			"user1:group1": {
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules: []*rules.RuleDesc{
					{
						Expr: "up",
					},
				},
			},
			"user2:group1": {
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user2",
				Rules: []*rules.RuleDesc{
					{
						Expr: "up",
					},
				},
			},
		},
	}
}

func (m *mockRuleStore) ListAllRuleGroups(ctx context.Context) (map[string]rules.RuleGroupList, error) {
	m.Lock()
	defer m.Unlock()

	userGroupMap := map[string]rules.RuleGroupList{}

	for id, rg := range m.rules {
		components := strings.Split(id, ":")
		if len(components) != 3 {
			continue
		}
		user := components[0]

		if _, exists := userGroupMap[user]; !exists {
			userGroupMap[user] = rules.RuleGroupList{}
		}
		userGroupMap[user] = append(userGroupMap[user], rg)
	}

	return userGroupMap, nil
}

func (m *mockRuleStore) ListRuleGroups(ctx context.Context, userID, namespace string) (rules.RuleGroupList, error) {
	m.Lock()
	defer m.Unlock()

	groupPrefix := userID + ":"

	namespaces := []string{}
	nss := rules.RuleGroupList{}
	for n := range m.rules {
		if strings.HasPrefix(n, groupPrefix) {
			components := strings.Split(n, ":")
			if len(components) != 3 {
				continue
			}
			namespaces = append(namespaces, components[1])
		}
	}

	if len(namespaces) == 0 {
		return nss, rules.ErrUserNotFound
	}

	for _, n := range namespaces {
		ns, err := m.getRuleNamespace(ctx, userID, n)
		if err != nil {
			continue
		}

		nss = append(nss, ns...)
	}

	return nss, nil
}

func (m *mockRuleStore) getRuleNamespace(ctx context.Context, userID string, namespace string) (rules.RuleGroupList, error) {
	groupPrefix := userID + ":" + namespace + ":"

	ns := rules.RuleGroupList{}
	for n, g := range m.rules {
		if strings.HasPrefix(n, groupPrefix) {
			ns = append(ns, g)
		}
	}

	if len(ns) == 0 {
		return ns, rules.ErrGroupNamespaceNotFound
	}

	return ns, nil
}

func (m *mockRuleStore) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (*rules.RuleGroupDesc, error) {
	m.Lock()
	defer m.Unlock()

	groupID := userID + ":" + namespace + ":" + group
	g, ok := m.rules[groupID]

	if !ok {
		return nil, rules.ErrGroupNotFound
	}

	return g, nil

}

func (m *mockRuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group *rules.RuleGroupDesc) error {
	m.Lock()
	defer m.Unlock()

	groupID := userID + ":" + namespace + ":" + group.Name
	m.rules[groupID] = group
	return nil
}

func (m *mockRuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	m.Lock()
	defer m.Unlock()

	groupID := userID + ":" + namespace + ":" + group
	delete(m.rules, groupID)
	return nil
}
