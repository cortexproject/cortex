package rulespb

import "github.com/prometheus/prometheus/model/rulefmt"

// RuleGroupList contains a set of rule groups
type RuleGroupList []*RuleGroupDesc

// RuleGroup is a rulefmt.RuleGroup extended with the Cortex-specific fields
// exposed through the ruler API.
type RuleGroup struct {
	rulefmt.RuleGroup `yaml:",inline"`
	// Tenants queried when evaluating the group. Empty means the owning tenant only.
	SrcTenants []string `yaml:"src_tenants,omitempty"`
}

// IsFederated returns true if the group queries data from explicitly listed tenants.
func (m *RuleGroupDesc) IsFederated() bool {
	return len(m.GetSrcTenants()) > 0
}

// Formatted returns the rule group list as prometheus rule groups mapped by
// namespace, without the Cortex-specific fields. It is meant for the rule files
// loaded by the prometheus rules manager.
func (l RuleGroupList) Formatted() map[string][]rulefmt.RuleGroup {
	ruleMap := map[string][]rulefmt.RuleGroup{}
	for _, g := range l {
		ruleMap[g.Namespace] = append(ruleMap[g.Namespace], FromProto(g).RuleGroup)
	}
	return ruleMap
}

// FormattedRuleGroups returns the rule group list as formatted rule groups
// mapped by namespace, keeping the Cortex-specific fields.
func (l RuleGroupList) FormattedRuleGroups() map[string][]RuleGroup {
	ruleMap := map[string][]RuleGroup{}
	for _, g := range l {
		ruleMap[g.Namespace] = append(ruleMap[g.Namespace], FromProto(g))
	}
	return ruleMap
}
