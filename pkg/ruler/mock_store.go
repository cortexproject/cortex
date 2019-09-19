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
