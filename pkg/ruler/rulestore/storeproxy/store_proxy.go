package storeproxy

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/tenant"
)

type MergeableStore struct {
	rulestore rulestore.RuleStore
	logger    log.Logger
}

func NewMergeableStore(rstore rulestore.RuleStore, logger log.Logger) rulestore.RuleStore {
	return &MergeableStore{
		rulestore: rstore,
		logger:    logger,
	}
}
func (s *MergeableStore) ListAllUsers(ctx context.Context) ([]string, error) {
	return s.rulestore.ListAllUsers(ctx)
}

func (s *MergeableStore) ListAllRuleGroups(ctx context.Context) (map[string]rulespb.RuleGroupList, error) {
	return s.rulestore.ListAllRuleGroups(ctx)

}

// return all the tenant rules when userID is a federation tenant(userID contains "|")
func (s *MergeableStore) ListRuleGroupsForUserAndNamespace(ctx context.Context, userID string, namespace string) (rulespb.RuleGroupList, error) {
	var rgs rulespb.RuleGroupList
	tids, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}

	for _, uid := range tids {
		trgs, err := s.rulestore.ListRuleGroupsForUserAndNamespace(ctx, uid, namespace)
		if err != nil {
			return nil, err
		}
		if trgs == nil {
			continue
		}
		for _, rule := range trgs {
			rgs = append(rgs, rule)
		}
	}

	return rgs, nil
}

func (s *MergeableStore) LoadRuleGroups(ctx context.Context, groupsToLoad map[string]rulespb.RuleGroupList) error {
	return s.rulestore.LoadRuleGroups(ctx, groupsToLoad)
}

// return all the tenant rules base on namespace and group when userID is a federation tenant(userID contains "|")
func (s *MergeableStore) GetRuleGroup(ctx context.Context, userID, namespace, group string) (*rulespb.RuleGroupDesc, error) {
	tids, err := tenant.TenantIDs(ctx)
	if err != nil {
		level.Error(s.logger).Log("get tenant error:", err)
		return nil, err
	}
	var rgs *rulespb.RuleGroupDesc
	for _, uid := range tids {
		trgs, err := s.rulestore.GetRuleGroup(ctx, uid, namespace, group)
		if err != nil {
			level.Error(s.logger).Log("get GetRuleGroup error:", err)
		}
		if trgs == nil {
			continue
		}
		if rgs == nil {
			rgs = trgs
		} else {
			for _, rule := range trgs.Rules {
				rgs.Rules = append(rgs.Rules, rule)
			}
		}
		rgs.User = userID
	}

	return rgs, nil

}
func (s *MergeableStore) SetRuleGroup(ctx context.Context, userID, namespace string, group *rulespb.RuleGroupDesc) error {
	return s.rulestore.SetRuleGroup(ctx, userID, namespace, group)
}

func (s *MergeableStore) DeleteRuleGroup(ctx context.Context, userID, namespace string, group string) error {
	return s.rulestore.DeleteRuleGroup(ctx, userID, namespace, group)
}

func (s *MergeableStore) DeleteNamespace(ctx context.Context, userID, namespace string) error {
	return s.rulestore.DeleteNamespace(ctx, userID, namespace)
}
