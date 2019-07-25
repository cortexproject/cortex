package rules

import (
	"context"
	time "time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

// TODO: Add a lazy rule group that only loads rules when they are needed
// TODO: The cortex project should implement a separate Group struct from
//       the prometheus project. This will allow for more precise instrumentation

// Group is used as a compatibility format between storage and evaluation
type Group struct {
	name      string
	namespace string
	user      string
	interval  time.Duration
	rules     []*RuleDesc

	// activeRules allows for the support of the configdb client
	// TODO: figure out a better way to accomplish this
	activeRules []rules.Rule
}

// NewRuleGroup returns a Group
func NewRuleGroup(name, namespace, user string, rls []rulefmt.Rule) *Group {
	return &Group{
		name:      name,
		namespace: namespace,
		user:      user,
		rules:     formattedRuleToProto(rls),
	}
}

// Rules returns eval ready prometheus rules
func (g *Group) Rules(ctx context.Context) ([]rules.Rule, error) {
	// Used to be compatible with configdb client
	if g.rules == nil && g.activeRules != nil {
		return g.activeRules, nil
	}

	rls := make([]rules.Rule, 0, len(g.rules))
	for _, rl := range g.rules {
		expr, err := promql.ParseExpr(rl.GetExpr())
		if err != nil {
			return nil, err
		}

		if rl.Alert != "" {
			rls = append(rls, rules.NewAlertingRule(
				rl.Alert,
				expr,
				*rl.GetFor(),
				client.FromLabelAdaptersToLabels(rl.Labels),
				client.FromLabelAdaptersToLabels(rl.Annotations),
				true,
				log.With(util.Logger, "alert", rl.Alert),
			))
			continue
		}
		rls = append(rls, rules.NewRecordingRule(
			rl.Record,
			expr,
			client.FromLabelAdaptersToLabels(rl.Labels),
		))
	}
	return rls, nil
}

// ID returns a unique group identifier with the namespace and name
func (g *Group) ID() string {
	return g.namespace + "/" + g.name
}

// Name returns the name of the rule group
func (g *Group) Name() string {
	return g.name
}

// Namespace returns the Namespace of the rule group
func (g *Group) Namespace() string {
	return g.namespace
}

// User returns the User of the rule group
func (g *Group) User() string {
	return g.user
}

// Formatted returns a prometheus rulefmt formatted rule group
func (g *Group) Formatted() rulefmt.RuleGroup {
	formattedRuleGroup := rulefmt.RuleGroup{
		Name:     g.name,
		Interval: model.Duration(g.interval),
		Rules:    make([]rulefmt.Rule, len(g.rules)),
	}

	for i, rl := range g.rules {
		formattedRuleGroup.Rules[i] = rulefmt.Rule{
			Record:      rl.GetRecord(),
			Alert:       rl.GetAlert(),
			Expr:        rl.GetExpr(),
			For:         model.Duration(*rl.GetFor()),
			Labels:      client.FromLabelAdaptersToLabels(rl.Labels).Map(),
			Annotations: client.FromLabelAdaptersToLabels(rl.Annotations).Map(),
		}
	}

	return formattedRuleGroup
}
