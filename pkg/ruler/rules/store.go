package rules

import (
	"context"
	"errors"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/client"

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

// ConfigRuleStore is a concrete implementation of RuleStore that sources rules from the config service
type ConfigRuleStore struct {
	configClient  client.Client
	since         configs.ID
	ruleGroupList map[string]RuleGroupList
}

// NewConfigRuleStore constructs a ConfigRuleStore
func NewConfigRuleStore(c client.Client) *ConfigRuleStore {
	return &ConfigRuleStore{
		configClient:  c,
		since:         0,
		ruleGroupList: make(map[string]RuleGroupList),
	}
}

// ListAllRuleGroups implements RuleStore
func (c *ConfigRuleStore) ListAllRuleGroups(ctx context.Context) (map[string]RuleGroupList, error) {

	configs, err := c.configClient.GetRules(ctx, c.since)

	if err != nil {
		return nil, err
	}

	for user, cfg := range configs {
		userRules := RuleGroupList{}
		if cfg.IsDeleted() {
			c.ruleGroupList[user] = RuleGroupList{}
		}
		rMap, err := cfg.Config.ParseFormatted()
		if err != nil {
			return nil, err
		}
		for file, rgs := range rMap {
			for _, rg := range rgs.Groups {
				userRules = append(userRules, ToProto(user, file, rg))
			}
		}
		c.ruleGroupList[user] = userRules
	}

	if err != nil {
		return nil, err
	}

	c.since = getLatestConfigID(configs, c.since)

	return c.ruleGroupList, nil
}

// getLatestConfigID gets the latest configs ID.
// max [latest, max (map getID cfgs)]
func getLatestConfigID(cfgs map[string]configs.VersionedRulesConfig, latest configs.ID) configs.ID {
	ret := latest
	for _, config := range cfgs {
		if config.ID > ret {
			ret = config.ID
		}
	}
	return ret
}
