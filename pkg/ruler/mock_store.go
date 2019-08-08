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

// RuleStore returns an RuleStore from the client
func (m *mockRuleStore) RuleStore() rules.RuleStore {
	return m
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
