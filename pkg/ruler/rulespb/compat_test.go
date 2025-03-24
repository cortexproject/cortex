package rulespb

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestProto(t *testing.T) {
	rules := make([]rulefmt.RuleNode, 0)

	alertNode := yaml.Node{}
	alertNode.SetString("test_rule")
	exprNode := yaml.Node{}
	exprNode.SetString("test_expr")

	testRule := rulefmt.RuleNode{
		Alert:         alertNode,
		Expr:          exprNode,
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

	desc := ToProto("test", "namespace", rg)

	assert.Equal(t, len(rules), len(desc.Rules))
	assert.Equal(t, 30*time.Second, *desc.QueryOffset)

	ruleDesc := desc.Rules[0]

	assert.Equal(t, "test_rule", ruleDesc.Alert)
	assert.Equal(t, "test_expr", ruleDesc.Expr)
	assert.Equal(t, time.Minute*2, ruleDesc.For)
	assert.Equal(t, time.Hour, ruleDesc.KeepFiringFor)

	formatted := FromProto(desc)
	assert.Equal(t, rg, formatted)
}
