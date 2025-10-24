package ruler

import (
	"testing"

	"github.com/prometheus/prometheus/model/rulefmt"
)

func TestValidateRuleGroup_AcceptsXFunctions(t *testing.T) {
	manager := &DefaultMultiTenantManager{}

	// Test rule with XFunction (should now pass)
	ruleGroupWithXFunc := rulefmt.RuleGroup{
		Name: "test_group",
		Rules: []rulefmt.Rule{
			{
				Alert: "TestAlert",
				Expr:  "xrate(cpu_usage[5m]) > 0.8", // XFunction
			},
		},
	}

	errs := manager.ValidateRuleGroup(ruleGroupWithXFunc)

	// Should have no validation errors now
	if len(errs) != 0 {
		t.Fatalf("Expected no validation errors for XFunction after fix, got: %v", errs)
	}
}

func TestValidateRuleGroup_AcceptsStandardFunctions(t *testing.T) {
	manager := &DefaultMultiTenantManager{}

	// Test rule with standard function (should pass)
	ruleGroupStandard := rulefmt.RuleGroup{
		Name: "test_group",
		Rules: []rulefmt.Rule{
			{
				Alert: "TestAlert",
				Expr:  "rate(cpu_usage[5m]) > 0.8", // Standard function
			},
		},
	}

	errs := manager.ValidateRuleGroup(ruleGroupStandard)

	// Should have no validation errors
	if len(errs) != 0 {
		t.Fatalf("Expected no validation errors for standard function, got: %v", errs)
	}
}
