package rules

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/pkg/rulefmt"
)

var (
	// ErrGroupNotFound is returned if a rule group does not exist
	ErrGroupNotFound = errors.New("group does not exist")
	// ErrGroupNamespaceNotFound is returned if a namespace does not exist
	ErrGroupNamespaceNotFound = errors.New("group namespace does not exist")
	// ErrUserNotFound is returned if the user does not currently exist
	ErrUserNotFound = errors.New("no rule groups found for user")
)

// RuleStore is used to store and retrieve rules
type RuleStore interface {
	ListAllRuleGroups(ctx context.Context) (map[string]RuleGroupList, error)
}

// RuleGroupList contains a set of rule groups
type RuleGroupList []*RuleGroupDesc

// Formatted returns the rule group list as a set of formatted rule groups mapped
// by namespace
func (l RuleGroupList) Formatted() map[string][]rulefmt.RuleGroup {
	ruleMap := map[string][]rulefmt.RuleGroup{}
	for _, g := range l {
		if _, exists := ruleMap[g.Namespace]; !exists {
			ruleMap[g.Namespace] = []rulefmt.RuleGroup{FromProto(g)}
			continue
		}
		ruleMap[g.Namespace] = append(ruleMap[g.Namespace], FromProto(g))

	}
	return ruleMap
}
