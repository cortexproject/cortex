package ruler

import (
	"context"
	"strings"
	"sync"

	"github.com/cortexproject/cortex/pkg/configs/storage/rules"
	"github.com/prometheus/prometheus/pkg/rulefmt"
)

type mockRuleStore struct {
	sync.Mutex
	rules map[string]rules.RuleGroup

	pollPayload map[string][]rules.RuleGroup
}

func (m *mockRuleStore) PollRules(ctx context.Context) (map[string][]rules.RuleGroup, error) {
	m.Lock()
	defer m.Unlock()
	pollPayload := m.pollPayload
	m.pollPayload = map[string][]rules.RuleGroup{}
	return pollPayload, nil
}

func (m *mockRuleStore) Stop() {}

// RuleStore returns an RuleStore from the client
func (m *mockRuleStore) RuleStore() rules.RuleStore {
	return m
}

func (m *mockRuleStore) ListRuleGroups(ctx context.Context, options rules.RuleStoreConditions) (rules.RuleGroupList, error) {
	m.Lock()
	defer m.Unlock()

	groupPrefix := options.UserID + ":"

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
		ns, err := m.getRuleNamespace(ctx, options.UserID, n)
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

func (m *mockRuleStore) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (rules.RuleGroup, error) {
	m.Lock()
	defer m.Unlock()

	groupID := userID + ":" + namespace + ":" + group
	g, ok := m.rules[groupID]

	if !ok {
		return nil, rules.ErrGroupNotFound
	}

	return g, nil

}

func (m *mockRuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group rulefmt.RuleGroup) error {
	m.Lock()
	defer m.Unlock()

	groupID := userID + ":" + namespace + ":" + group.Name
	m.rules[groupID] = rules.NewRuleGroup(group.Name, namespace, userID, group.Rules)
	return nil
}

func (m *mockRuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	m.Lock()
	defer m.Unlock()

	groupID := userID + ":" + namespace + ":" + group
	delete(m.rules, groupID)
	return nil
}
