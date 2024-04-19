package ruler

import (
	"context"
	"net/url"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promRules "github.com/prometheus/prometheus/rules"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
)

// rulesBackupManager is an in-memory store that holds rulespb.RuleGroupList of multiple users. It only stores the
// data, it DOESN'T evaluate.
type rulesBackupManager struct {
	inMemoryRuleGroupsBackup map[string]rulespb.RuleGroupList
	cfg                      Config

	logger log.Logger

	backupRuleGroup *prometheus.GaugeVec
}

func newRulesBackupManager(cfg Config, logger log.Logger, reg prometheus.Registerer) *rulesBackupManager {
	return &rulesBackupManager{
		inMemoryRuleGroupsBackup: make(map[string]rulespb.RuleGroupList),
		cfg:                      cfg,
		logger:                   logger,

		backupRuleGroup: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_backup_rule_group",
			Help:      "Boolean set to 1 indicating the ruler stores the rule group as backup.",
		}, []string{"user", "rule_group"}),
	}
}

// setRuleGroups updates the map[string]rulespb.RuleGroupList that the rulesBackupManager stores in memory.
func (r *rulesBackupManager) setRuleGroups(_ context.Context, ruleGroups map[string]rulespb.RuleGroupList) {
	r.updateMetrics(ruleGroups)
	r.inMemoryRuleGroupsBackup = ruleGroups
}

// getRuleGroups returns the rulespb.RuleGroupList that rulesBackupManager stores for a given user
func (r *rulesBackupManager) getRuleGroups(userID string) rulespb.RuleGroupList {
	var result rulespb.RuleGroupList
	if groups, exists := r.inMemoryRuleGroupsBackup[userID]; exists {
		result = groups
	}
	return result
}

// updateMetrics updates the ruler_backup_rule_group metric by adding new groups that were backed up and removing
// those that are removed from the backup.
func (r *rulesBackupManager) updateMetrics(newBackupGroups map[string]rulespb.RuleGroupList) {
	for user, groups := range newBackupGroups {
		keptGroups := make(map[string]struct{})
		for _, g := range groups {
			fullFileName := r.getFilePathForGroup(g, user)
			key := promRules.GroupKey(fullFileName, g.GetName())
			r.backupRuleGroup.WithLabelValues(user, key).Set(1)
			keptGroups[key] = struct{}{}
		}
		oldGroups := r.inMemoryRuleGroupsBackup[user]
		for _, g := range oldGroups {
			fullFileName := r.getFilePathForGroup(g, user)
			key := promRules.GroupKey(fullFileName, g.GetName())
			if _, exists := keptGroups[key]; !exists {
				r.backupRuleGroup.DeleteLabelValues(user, key)
			}
		}
	}

	for user, groups := range r.inMemoryRuleGroupsBackup {
		if _, exists := newBackupGroups[user]; exists {
			continue
		}
		for _, g := range groups {
			fullFileName := r.getFilePathForGroup(g, user)
			key := promRules.GroupKey(fullFileName, g.GetName())
			r.backupRuleGroup.DeleteLabelValues(user, key)
		}
	}
}

// getFilePathForGroup returns the supposed file path of the group if it was being evaluated.
// This is based on how mapper.go generates file paths. This can be used to generate value similar to the one returned
// by prometheus Group.File() method.
func (r *rulesBackupManager) getFilePathForGroup(g *rulespb.RuleGroupDesc, user string) string {
	dirPath := filepath.Join(r.cfg.RulePath, user)
	encodedFileName := url.PathEscape(g.GetNamespace())
	return filepath.Join(dirPath, encodedFileName)
}
