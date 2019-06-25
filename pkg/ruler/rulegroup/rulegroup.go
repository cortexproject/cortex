package rulegroup

import (
	"context"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/prometheus/prometheus/rules"
)

// TODO: Add a lazy rule group that only loads rules when they are needed

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
	return rg.user + "/" + rg.namespace + "/" + rg.name
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
