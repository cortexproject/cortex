package ruler

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
)

// rulesBackupManager is an in-memory store that holds rulespb.RuleGroupList of multiple users. It only stores the
// data, it DOESN'T evaluate.
type rulesBackupManager struct {
	inMemoryRuleGroupsBackup map[string]rulespb.RuleGroupList
	cfg                      Config

	logger log.Logger

	backupRules      *prometheus.GaugeVec
	backupRuleGroups *prometheus.GaugeVec
}

func newRulesBackupManager(cfg Config, logger log.Logger, reg prometheus.Registerer) *rulesBackupManager {
	return &rulesBackupManager{
		inMemoryRuleGroupsBackup: make(map[string]rulespb.RuleGroupList),
		cfg:                      cfg,
		logger:                   logger,

		backupRules: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_backup_rules",
			Help:      "The number of rules stored as backup.",
		}, []string{"user"}),
		backupRuleGroups: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_backup_rule_groups",
			Help:      "The number of rule groups stored as backup.",
		}, []string{"user"}),
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

// updateMetrics updates the ruler_backup_rules metric by updating the number of rules that were backed up and removing
// the users whose rules are no longer backed up
func (r *rulesBackupManager) updateMetrics(newBackupGroups map[string]rulespb.RuleGroupList) {
	for user, groups := range newBackupGroups {
		totalRules := 0
		for _, g := range groups {
			totalRules += len(g.Rules)
		}
		r.backupRules.WithLabelValues(user).Set(float64(totalRules))
		r.backupRuleGroups.WithLabelValues(user).Set(float64(len(groups)))
	}

	for user := range r.inMemoryRuleGroupsBackup {
		if _, exists := newBackupGroups[user]; !exists {
			r.backupRules.DeleteLabelValues(user)
			r.backupRuleGroups.DeleteLabelValues(user)
		}
	}
}
