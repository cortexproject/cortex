package configs

import (
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/cortex/pkg/util"
)

// An ID is the ID of a single users's Cortex configuration. When a
// configuration changes, it gets a new ID.
type ID int

// RuleFormatVersion indicates which Prometheus rule format (v1 vs. v2) to use in parsing.
type RuleFormatVersion int

const (
	// RuleFormatV1 is the Prometheus 1.x rule format.
	RuleFormatV1 RuleFormatVersion = iota
	// RuleFormatV2 is the Prometheus 2.x rule format.
	RuleFormatV2 RuleFormatVersion = iota
)

// String implements flag.Value.
func (v RuleFormatVersion) String() string {
	switch v {
	case RuleFormatV1:
		return "1"
	case RuleFormatV2:
		return "2"
	default:
		return "<unknown>"
	}
}

// Set implements flag.Value.
func (v *RuleFormatVersion) Set(s string) error {
	switch s {
	case "1":
		*v = RuleFormatV1
	case "2":
		*v = RuleFormatV2
	default:
		return fmt.Errorf("invalid rule format version %q", s)
	}
	return nil
}

// A Config is a Cortex configuration for a single user.
type Config struct {
	// RulesFiles maps from a rules filename to file contents.
	RulesFiles         RulesConfig `json:"rules_files"`
	AlertmanagerConfig string      `json:"alertmanager_config"`
}

// View is what's returned from the Weave Cloud configs service
// when we ask for all Cortex configurations.
//
// The configs service is essentially a JSON blob store that gives each
// _version_ of a configuration a unique ID and guarantees that later versions
// have greater IDs.
type View struct {
	ID        ID        `json:"id"`
	Config    Config    `json:"config"`
	DeletedAt time.Time `json:"deleted_at"`
}

// GetVersionedRulesConfig specializes the view to just the rules config.
func (v View) GetVersionedRulesConfig() *VersionedRulesConfig {
	if v.Config.RulesFiles == nil {
		return nil
	}
	return &VersionedRulesConfig{
		ID:        v.ID,
		Config:    v.Config.RulesFiles,
		DeletedAt: v.DeletedAt,
	}
}

// RulesConfig are the set of rules files for a particular organization.
type RulesConfig map[string]string

// Equal compares two RulesConfigs for equality.
//
// instance Eq RulesConfig
func (c RulesConfig) Equal(o RulesConfig) bool {
	if len(o) != len(c) {
		return false
	}
	for k, v1 := range c {
		v2, ok := o[k]
		if !ok || v1 != v2 {
			return false
		}
	}
	return true
}

// Parse parses and validates the content of the rule files in a RulesConfig
// according to the passed rule format version.
func (c RulesConfig) Parse(v RuleFormatVersion) (map[string][]rules.Rule, error) {
	switch v {
	case RuleFormatV1:
		return c.ParseV1()
	case RuleFormatV2:
		return c.ParseV2()
	default:
		panic("unknown rule format")
	}
}

// ParseV2 parses and validates the content of the rule files in a RulesConfig
// according to the Prometheus 2.x rule format.
//
// NOTE: On one hand, we cannot return fully-fledged lists of rules.Group
// here yet, as creating a rules.Group requires already
// passing in rules.ManagerOptions options (which in turn require a
// notifier, appender, etc.), which we do not want to create simply
// for parsing. On the other hand, we should not return barebones
// rulefmt.RuleGroup sets here either, as only a fully-converted rules.Rule
// is able to track alert states over multiple rule evaluations. The caller
// would otherwise have to ensure to convert the rulefmt.RuleGroup only exactly
// once, not for every evaluation (or risk losing alert pending states). So
// it's probably better to just return a set of rules.Rule here.
func (c RulesConfig) ParseV2() (map[string][]rules.Rule, error) {
	groups := map[string][]rules.Rule{}

	for fn, content := range c {
		rgs, errs := rulefmt.Parse([]byte(content))
		if len(errs) > 0 {
			return nil, fmt.Errorf("error parsing %s: %v", fn, errs[0])
		}

		for _, rg := range rgs.Groups {
			rls := make([]rules.Rule, 0, len(rg.Rules))
			for _, rl := range rg.Rules {
				expr, err := promql.ParseExpr(rl.Expr)
				if err != nil {
					return nil, err
				}

				if rl.Alert != "" {
					rls = append(rls, rules.NewAlertingRule(
						rl.Alert,
						expr,
						time.Duration(rl.For),
						labels.FromMap(rl.Labels),
						labels.FromMap(rl.Annotations),
						log.With(util.Logger, "alert", rl.Alert),
					))
					continue
				}
				rls = append(rls, rules.NewRecordingRule(
					rl.Record,
					expr,
					labels.FromMap(rl.Labels),
				))
			}

			// Group names have to be unique in Prometheus, but only within one rules file.
			groups[rg.Name+";"+fn] = rls
		}
	}

	return groups, nil
}

// ParseV1 parses and validates the content of the rule files in a RulesConfig
// according to the Prometheus 1.x rule format.
//
// The same comment about rule groups as on ParseV2() applies here.
func (c RulesConfig) ParseV1() (map[string][]rules.Rule, error) {
	result := map[string][]rules.Rule{}
	for fn, content := range c {
		stmts, err := promql.ParseStmts(content)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %s", fn, err)
		}
		ra := []rules.Rule{}
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
			ra = append(ra, rule)
		}
		result[fn] = ra
	}
	return result, nil
}

// VersionedRulesConfig is a RulesConfig together with a version.
// `data Versioned a = Versioned { id :: ID , config :: a }`
type VersionedRulesConfig struct {
	ID        ID          `json:"id"`
	Config    RulesConfig `json:"config"`
	DeletedAt time.Time   `json:"deleted_at"`
}

// IsDeleted tells you if the config is deleted.
func (vr VersionedRulesConfig) IsDeleted() bool {
	return !vr.DeletedAt.IsZero()
}
