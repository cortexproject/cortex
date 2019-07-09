package rulegroup

import (
	"context"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/prometheus/prometheus/rules"
)

// TODO: Add a lazy rule group that only loads rules when they are needed
// TODO: The cortex project should implement a separate Group struct from
//       the prometheus project. This will allow for more precise instrumentation

type ruleGroup struct {
	name      string
	namespace string
	user      string

	rules []rules.Rule
}

func (rg *ruleGroup) Rules(ctx context.Context) ([]rules.Rule, error) {
	return rg.rules, nil
}

func (rg *ruleGroup) Name() string {
	return rg.namespace + "/" + rg.name
}

func (rg *ruleGroup) User() string {
	return rg.user
}

// NewRuleGroup returns a rulegroup
func NewRuleGroup(name, namespace, user string, rules []rules.Rule) configs.RuleGroup {
	return &ruleGroup{
		name:      name,
		namespace: namespace,
		user:      user,
		rules:     rules,
	}
}
