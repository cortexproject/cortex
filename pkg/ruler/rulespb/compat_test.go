package rulespb

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestProto(t *testing.T) {
	rules := make([]rulefmt.Rule, 0)

	testRule := rulefmt.Rule{
		Alert:         "test_rule",
		Expr:          "test_expr",
		Labels:        map[string]string{"label1": "value1"},
		Annotations:   map[string]string{"key1": "value1"},
		For:           model.Duration(time.Minute * 2),
		KeepFiringFor: model.Duration(time.Hour),
	}

	rules = append(rules, testRule)

	queryOffset := model.Duration(30 * time.Second)
	rg := rulefmt.RuleGroup{
		Name:        "group1",
		Rules:       rules,
		Interval:    model.Duration(time.Minute),
		QueryOffset: &queryOffset,
		Labels:      map[string]string{},
	}

	desc := ToProto("test", "namespace", RuleGroup{RuleGroup: rg})

	assert.Equal(t, len(rules), len(desc.Rules))
	assert.Equal(t, 30*time.Second, *desc.QueryOffset)

	ruleDesc := desc.Rules[0]

	assert.Equal(t, "test_rule", ruleDesc.Alert)
	assert.Equal(t, "test_expr", ruleDesc.Expr)
	assert.Equal(t, time.Minute*2, ruleDesc.For)
	assert.Equal(t, time.Hour, ruleDesc.KeepFiringFor)

	formatted := FromProto(desc)
	assert.Equal(t, rg, formatted.RuleGroup)
	assert.Empty(t, formatted.SourceTenants)
}

func TestProtoRuleGroup(t *testing.T) {
	rg := RuleGroup{
		Name:          "group1",
		Interval:      model.Duration(time.Minute),
		Rules:         []rulefmt.Rule{{Record: "test_record", Expr: "test_expr", Labels: map[string]string{}, Annotations: map[string]string{}}},
		Labels:        map[string]string{},
		SourceTenants: []string{"team-a", "team-b"},
	}

	desc := ToProto("test", "namespace", rg)
	assert.Equal(t, []string{"team-a", "team-b"}, desc.SourceTenants)
	assert.True(t, desc.IsFederated())

	assert.Equal(t, rg, FromProto(desc))

	// Groups without source tenants are not federated.
	plain := ToProto("test", "namespace", RuleGroup{RuleGroup: rg.RuleGroup})
	assert.False(t, plain.IsFederated())
	assert.Empty(t, FromProto(plain).SourceTenants)
}

func TestRuleGroupYAML(t *testing.T) {
	in := "name: group1\ninterval: 1m\nsource_tenants:\n    - team-a\n    - team-b\nrules:\n    - record: test_record\n      expr: test_expr\n"

	rg := RuleGroup{}
	require.NoError(t, yaml.Unmarshal([]byte(in), &rg))
	assert.Equal(t, "group1", rg.Name)
	assert.Equal(t, []string{"team-a", "team-b"}, rg.SourceTenants)

	out, err := yaml.Marshal(rg)
	require.NoError(t, err)
	assert.Equal(t, "name: group1\ninterval: 1m\nrules:\n    - record: test_record\n      expr: test_expr\nsource_tenants:\n    - team-a\n    - team-b\n", string(out))

	// source_tenants is omitted when empty, keeping the output of plain groups unchanged.
	out, err = yaml.Marshal(RuleGroup{RuleGroup: rg.RuleGroup})
	require.NoError(t, err)
	assert.NotContains(t, string(out), "source_tenants")
}
