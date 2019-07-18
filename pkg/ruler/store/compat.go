package store

import (
	time "time"

	"github.com/cortexproject/cortex/pkg/ingester/client"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/rules"
)

// ProtoRuleUpdateDescFactory makes new RuleUpdateDesc
func ProtoRuleUpdateDescFactory() proto.Message {
	return NewRuleUpdateDesc()
}

// NewRuleUpdateDesc returns an empty *distributor.RuleUpdateDesc.
func NewRuleUpdateDesc() *RuleUpdateDesc {
	return &RuleUpdateDesc{}
}

// ToProto transforms a formatted prometheus rulegroup to a rule group protobuf
func ToProto(user string, namespace string, rl rulefmt.RuleGroup) RuleGroupDesc {
	dur := time.Duration(rl.Interval)
	rg := RuleGroupDesc{
		Name:      rl.Name,
		Namespace: namespace,
		Interval:  &dur,
		Rules:     formattedRuleToProto(rl.Rules),
		User:      user,
	}
	return rg
}

func formattedRuleToProto(rls []rulefmt.Rule) []*Rule {
	rules := make([]*Rule, len(rls))
	for i := range rls {
		f := time.Duration(rls[i].For)

		rules[i] = &Rule{
			Expr:   rls[i].Expr,
			Record: rls[i].Record,
			Alert:  rls[i].Alert,

			For:         &f,
			Labels:      client.FromLabelsToLabelAdapaters(labels.FromMap(rls[i].Labels)),
			Annotations: client.FromLabelsToLabelAdapaters(labels.FromMap(rls[i].Labels)),
		}
	}

	return rules
}

// FromProto generates a rulefmt RuleGroup
func FromProto(rg *RuleGroupDesc) *rulefmt.RuleGroup {
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

	return &formattedRuleGroup
}

// ToRuleGroup returns a functional rulegroup from a proto
func ToRuleGroup(rg *RuleGroupDesc) *Group {
	return &Group{
		name:      rg.GetName(),
		namespace: rg.GetNamespace(),
		user:      rg.GetUser(),
		interval:  *rg.Interval,
		rules:     rg.Rules,
	}
}

// FormattedToRuleGroup transforms a formatted prometheus rulegroup to a rule group protobuf
func FormattedToRuleGroup(user string, namespace string, name string, rls []rules.Rule) *Group {
	return &Group{
		name:        name,
		namespace:   namespace,
		user:        user,
		activeRules: rls,
	}
}
