package rulegroup

import (
	time "time"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

// ToProto transforms a formatted prometheus rulegroup to a rule group protobuf
func ToProto(namespace string, rl rulefmt.RuleGroup) RuleGroup {
	dur := time.Duration(rl.Interval)
	rg := RuleGroup{
		Name:      rl.Name,
		Namespace: namespace,
		Interval:  &dur,
	}

	rules := make([]*Rule, len(rl.Rules))
	for i := range rl.Rules {
		f := time.Duration(rl.Rules[i].For)

		rules[i] = &Rule{
			Expr:   rl.Rules[i].Expr,
			Record: rl.Rules[i].Record,
			Alert:  rl.Rules[i].Alert,

			For:         &f,
			Labels:      client.FromLabelsToLabelAdapaters(labels.FromMap(rl.Rules[i].Labels)),
			Annotations: client.FromLabelsToLabelAdapaters(labels.FromMap(rl.Rules[i].Labels)),
		}
	}

	return rg
}

// FromProto generates a rulefmt RuleGroup
func FromProto(rg *RuleGroup) rulefmt.RuleGroup {
	formattedRuleGroup := rulefmt.RuleGroup{
		Name:     rg.GetName(),
		Interval: model.Duration(*rg.Interval),
		Rules:    make([]rulefmt.Rule, len(rg.GetRules())),
	}

	for i, rl := range rg.GetRules() {
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

// GenerateRuleGroup returns a functional rulegroup from a proto
func GenerateRuleGroup(userID string, rg *RuleGroup) (configs.RuleGroup, error) {
	rls := make([]rules.Rule, 0, len(rg.Rules))
	for _, rl := range rg.Rules {
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

	return NewRuleGroup(
		rg.GetName(),
		rg.GetNamespace(),
		userID,
		rls,
	), nil
}
