package configs

import (
	"fmt"

	amConfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/cortex/pkg/util"
)

// An ID is the ID of a single users's Cortex configuration. When a
// configuration changes, it gets a new ID.
type ID int

// A Config is a Cortex configuration for a single user.
type Config struct {
	// RulesFiles maps from a rules filename to file contents.
	RulesFiles         RulesConfig        `json:"rules_files"`
	AlertmanagerConfig AlertmanagerConfig `json:"alertmanager_config"`
}

// View is what's returned from the Weave Cloud configs service
// when we ask for all Cortex configurations.
//
// The configs service is essentially a JSON blob store that gives each
// _version_ of a configuration a unique ID and guarantees that later versions
// have greater IDs.
type View struct {
	ID     ID     `json:"id"`
	Config Config `json:"config"`
}

// GetVersionedAlertmanagerConfig specializes the view to just an alertmanager
// config.
func (v View) GetVersionedAlertmanagerConfig() VersionedAlertmanagerConfig {
	return VersionedAlertmanagerConfig{
		ID:     v.ID,
		Config: v.Config.AlertmanagerConfig,
	}
}

// GetVersionedRulesConfig specializes the view to just the rules config.
func (v View) GetVersionedRulesConfig() VersionedRulesConfig {
	return VersionedRulesConfig{
		ID:     v.ID,
		Config: v.Config.RulesFiles,
	}
}

// AlertmanagerConfig is an alertmanager config.
type AlertmanagerConfig string

// Parse an alertmanager config.
func (c AlertmanagerConfig) Parse() (*amConfig.Config, error) {
	cfg, err := amConfig.Load(string(c))
	if err != nil {
		return nil, fmt.Errorf("error parsing Alertmanager config: %s", err)
	}
	return cfg, nil
}

// VersionedAlertmanagerConfig is an AlertmanagerConfig together with a version.
type VersionedAlertmanagerConfig struct {
	ID     ID                 `json:"id"`
	Config AlertmanagerConfig `json:"config"`
}

// RulesConfig are the set of rules files for a particular organization.
type RulesConfig map[string]string

// Parse rules from the Cortex configuration.
//
// Strongly inspired by `loadGroups` in Prometheus.
func (c RulesConfig) Parse() ([]rules.Rule, error) {
	result := []rules.Rule{}
	for fn, content := range c {
		stmts, err := promql.ParseStmts(content)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %s", fn, err)
		}

		for _, stmt := range stmts {
			var rule rules.Rule

			switch r := stmt.(type) {
			case *promql.AlertStmt:
				rule = rules.NewAlertingRule(r.Name, r.Expr, r.Duration, r.Labels, r.Annotations, util.Logger)

			case *promql.RecordStmt:
				rule = rules.NewRecordingRule(r.Name, r.Expr, r.Labels)

			default:
				return nil, fmt.Errorf("ruler.GetRules: unknown statement type")
			}
			result = append(result, rule)
		}
	}
	return result, nil
}

// VersionedRulesConfig is a RulesConfig together with a version.
// `data Versioned a = Versioned { id :: ID , config :: a }`
type VersionedRulesConfig struct {
	ID     ID          `json:"id"`
	Config RulesConfig `json:"config"`
}
