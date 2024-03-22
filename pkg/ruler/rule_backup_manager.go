package ruler

import (
	"context"
	"errors"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"
	promRules "github.com/prometheus/prometheus/rules"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
)

// Implements GroupLoader interface but instead of reading from a file when Load is called, it returns the
// rulefmt.RuleGroup it has stored
type loader struct {
	ruleGroups map[string][]rulefmt.RuleGroup
}

func (r *loader) Load(identifier string) (*rulefmt.RuleGroups, []error) {
	return &rulefmt.RuleGroups{
		Groups: r.ruleGroups[identifier],
	}, nil
}

func (r *loader) Parse(query string) (parser.Expr, error) {
	return parser.ParseExpr(query)
}

// rulesBackupManager is an in-memory store that holds []promRules.Group of multiple users. It only stores the Groups,
// it doesn't evaluate them.
type rulesBackupManager struct {
	backupRuleGroupsMtx      sync.RWMutex
	inMemoryRuleGroupsBackup map[string][]*promRules.Group
	cfg                      Config

	logger log.Logger

	backupGroupRulesCount      *prometheus.GaugeVec
	lastBackupReloadSuccessful *prometheus.GaugeVec
}

func newRulesBackupManager(cfg Config, logger log.Logger, reg prometheus.Registerer) *rulesBackupManager {
	return &rulesBackupManager{
		inMemoryRuleGroupsBackup: make(map[string][]*promRules.Group),
		cfg:                      cfg,
		logger:                   logger,

		backupGroupRulesCount: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_backup_rule_group_rules",
			Help:      "The number of backed up rules",
		}, []string{"user", "rule_group"}),
		lastBackupReloadSuccessful: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_backup_last_reload_successful",
			Help:      "Boolean set to 1 whenever the last configuration reload attempt was successful.",
		}, []string{"user"}),
	}
}

// setRuleGroups updates the map[string][]*promRules.Group that the rulesBackupManager stores in memory.
func (r *rulesBackupManager) setRuleGroups(_ context.Context, ruleGroups map[string]rulespb.RuleGroupList) {
	backupRuleGroups := make(map[string][]*promRules.Group)
	for user, groups := range ruleGroups {
		promGroups, err := r.ruleGroupListToPromGroups(user, groups)
		if err != nil {
			r.lastBackupReloadSuccessful.WithLabelValues(user).Set(0)
			level.Error(r.logger).Log("msg", "unable to back up rules", "user", user, "err", err)
			continue
		}
		backupRuleGroups[user] = promGroups
		r.lastBackupReloadSuccessful.WithLabelValues(user).Set(1)
	}
	r.backupRuleGroupsMtx.Lock()
	defer r.backupRuleGroupsMtx.Unlock()
	r.updateMetrics(backupRuleGroups)
	r.inMemoryRuleGroupsBackup = backupRuleGroups
}

// ruleGroupListToPromGroups converts rulespb.RuleGroupList to []*promRules.Group by creating a single use
// promRules.Manager and calling its LoadGroups method.
func (r *rulesBackupManager) ruleGroupListToPromGroups(user string, ruleGroups rulespb.RuleGroupList) ([]*promRules.Group, error) {
	rgs := ruleGroups.Formatted()

	loader := &loader{
		ruleGroups: rgs,
	}
	promManager := promRules.NewManager(&promRules.ManagerOptions{
		ExternalURL: r.cfg.ExternalURL.URL,
		GroupLoader: loader,
	})

	namespaces := make([]string, 0, len(rgs))
	for k := range rgs {
		namespaces = append(namespaces, k)
	}
	loadedGroups, errs := promManager.LoadGroups(r.cfg.EvaluationInterval, r.cfg.ExternalLabels, r.cfg.ExternalURL.String(), nil, namespaces...)
	if errs != nil {
		for _, e := range errs {
			level.Error(r.logger).Log("msg", "loading groups to backup failed", "user", user, "namespaces", namespaces, "err", e)
		}
		return nil, errors.New("error loading rules to backup")
	}

	groups := make([]*promRules.Group, 0, len(loadedGroups))
	for _, g := range loadedGroups {
		groups = append(groups, g)
	}
	return groups, nil
}

// getRuleGroups returns the []*promRules.Group that rulesBackupManager stores for a given user
func (r *rulesBackupManager) getRuleGroups(userID string) []*promRules.Group {
	var result []*promRules.Group
	r.backupRuleGroupsMtx.RLock()
	defer r.backupRuleGroupsMtx.RUnlock()
	if groups, exists := r.inMemoryRuleGroupsBackup[userID]; exists {
		result = groups
	}
	return result
}

func (r *rulesBackupManager) updateMetrics(newBackupGroups map[string][]*promRules.Group) {
	for user, groups := range newBackupGroups {
		keptGroups := make(map[string][]interface{})
		for _, g := range groups {
			key := promRules.GroupKey(g.File(), g.Name())
			r.backupGroupRulesCount.WithLabelValues(user, key).Set(float64(len(g.Rules())))
			keptGroups[key] = nil
		}
		oldGroups := r.inMemoryRuleGroupsBackup[user]
		for _, g := range oldGroups {
			key := promRules.GroupKey(g.File(), g.Name())
			if _, exists := keptGroups[key]; !exists {
				r.backupGroupRulesCount.DeleteLabelValues(user, key)
			}
		}
	}

	for user, groups := range r.inMemoryRuleGroupsBackup {
		if _, exists := newBackupGroups[user]; exists {
			continue
		}
		for _, g := range groups {
			key := promRules.GroupKey(g.File(), g.Name())
			r.backupGroupRulesCount.DeleteLabelValues(user, key)
		}
		r.lastBackupReloadSuccessful.DeleteLabelValues(user)
	}
}
