package group

import (
	"context"

	"github.com/prometheus/prometheus/rules"
)

// TODO: Add a lazy rule group that only loads rules when they are needed
// TODO: The cortex project should implement a separate Group struct from
//       the prometheus project. This will allow for more precise instrumentation

type Group struct {
	name      string
	namespace string
	user      string

	rules []rules.Rule
}

func (rg *Group) Rules(ctx context.Context) ([]rules.Rule, error) {
	return rg.rules, nil
}

func (rg *Group) Name() string {
	return rg.namespace + "/" + rg.name
}

func (rg *Group) User() string {
	return rg.user
}

// NewRuleGroup returns a rulegroup
func NewRuleGroup(name, namespace, user string, rules []rules.Rule) *Group {
	return &Group{
		name:      name,
		namespace: namespace,
		user:      user,
		rules:     rules,
	}
}
