package rules

import (
	time "time"

	"github.com/cortexproject/cortex/pkg/ingester/client"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
)

// ToProto transforms a formatted prometheus rulegroup to a rule group protobuf
func ToProto(user string, namespace string, rl rulefmt.RuleGroup) *RuleGroupDesc {
	dur := time.Duration(rl.Interval)
	rg := RuleGroupDesc{
		Name:      rl.Name,
		Namespace: namespace,
		Interval:  &dur,
		Rules:     formattedRuleToProto(rl.Rules),
		User:      user,
	}
	return &rg
}

func formattedRuleToProto(rls []rulefmt.Rule) []*RuleDesc {
	rules := make([]*RuleDesc, len(rls))
	for i := range rls {
		f := time.Duration(rls[i].For)

		rules[i] = &RuleDesc{
			Expr:   rls[i].Expr,
			Record: rls[i].Record,
			Alert:  rls[i].Alert,

			For:         &f,
			Labels:      client.FromLabelsToLabelAdapaters(labels.FromMap(rls[i].Labels)),
			Annotations: client.FromLabelsToLabelAdapaters(labels.FromMap(rls[i].Annotations)),
		}
	}

	return rules
}

// FromProto generates a rulefmt RuleGroup
func FromProto(rg *RuleGroupDesc) rulefmt.RuleGroup {
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
