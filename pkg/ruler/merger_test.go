package ruler

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
)

func TestMergeGroupStateDesc(t *testing.T) {
	curTime := time.Now()
	r := rulespb.RuleDesc{
		Expr: "1 > 1",
	}
	g1 := rulespb.RuleGroupDesc{
		Name:      "g1",
		Namespace: "ns1",
	}
	g2 := rulespb.RuleGroupDesc{
		Name:      "g2",
		Namespace: "ns1",
	}
	rs1 := RuleStateDesc{
		Rule:                &r,
		EvaluationTimestamp: curTime,
	}
	rs1NotRun := RuleStateDesc{
		Rule: &r,
	}
	rs2 := RuleStateDesc{
		Rule:                &r,
		EvaluationTimestamp: curTime,
	}
	rs2NotRun := RuleStateDesc{
		Rule: &r,
	}
	rs3 := RuleStateDesc{
		Rule:                &r,
		EvaluationTimestamp: curTime.Add(10 * time.Second),
	}

	gs1 := GroupStateDesc{
		Group:               &g1,
		ActiveRules:         []*RuleStateDesc{&rs1, &rs2},
		EvaluationTimestamp: curTime,
	}
	gs1NotRun := GroupStateDesc{
		Group:       &g1,
		ActiveRules: []*RuleStateDesc{&rs1NotRun, &rs2NotRun},
	}
	gs2 := GroupStateDesc{
		Group:               &g2,
		ActiveRules:         []*RuleStateDesc{&rs1, &rs2},
		EvaluationTimestamp: curTime,
	}
	gs2NotRun := GroupStateDesc{
		Group:       &g2,
		ActiveRules: []*RuleStateDesc{&rs1NotRun, &rs2NotRun},
	}
	gs3 := GroupStateDesc{
		Group:               &g2,
		ActiveRules:         []*RuleStateDesc{&rs1, &rs3},
		EvaluationTimestamp: curTime,
	}

	type testCase struct {
		input          []*RulesResponse
		expectedOutput *RulesResponse
		maxRuleGroups  int32
	}

	testCases := map[string]testCase{
		"No duplicate": {
			input: []*RulesResponse{
				{
					Groups:    []*GroupStateDesc{&gs1, &gs2},
					NextToken: "",
				},
			},
			expectedOutput: &RulesResponse{
				Groups:    []*GroupStateDesc{&gs1, &gs2},
				NextToken: "",
			},
			maxRuleGroups: 2,
		},
		"No duplicate but not evaluated": {
			input: []*RulesResponse{
				{
					Groups:    []*GroupStateDesc{&gs1NotRun, &gs2NotRun},
					NextToken: "",
				},
			},
			expectedOutput: &RulesResponse{
				Groups:    []*GroupStateDesc{&gs1NotRun, &gs2NotRun},
				NextToken: "",
			},
			maxRuleGroups: 2,
		},
		"With exact duplicate": {
			input: []*RulesResponse{
				{
					Groups:    []*GroupStateDesc{&gs1, &gs2NotRun},
					NextToken: "",
				},
				{
					Groups:    []*GroupStateDesc{&gs1, &gs2NotRun},
					NextToken: "",
				},
			},
			expectedOutput: &RulesResponse{
				Groups:    []*GroupStateDesc{&gs1, &gs2NotRun},
				NextToken: "",
			},
			maxRuleGroups: 2,
		},
		"With duplicates that are not evaluated": {
			input: []*RulesResponse{
				{
					Groups:    []*GroupStateDesc{&gs1, &gs2},
					NextToken: "",
				},
				{
					Groups:    []*GroupStateDesc{&gs1NotRun},
					NextToken: "",
				},
				{
					Groups:    []*GroupStateDesc{&gs2NotRun},
					NextToken: "",
				},
			},
			expectedOutput: &RulesResponse{
				Groups:    []*GroupStateDesc{&gs1, &gs2},
				NextToken: "",
			},
			maxRuleGroups: 2,
		},
		"With duplicate with a new newer rule evaluation": {
			input: []*RulesResponse{
				{
					Groups:    []*GroupStateDesc{&gs3},
					NextToken: GetRuleGroupNextToken(gs3.Group.Name, gs3.Group.Name),
				},
				{
					Groups:    []*GroupStateDesc{&gs1},
					NextToken: "",
				},
				{
					Groups:    []*GroupStateDesc{&gs2},
					NextToken: "",
				},
				{
					Groups:    []*GroupStateDesc{&gs1NotRun},
					NextToken: "",
				},
			},
			expectedOutput: &RulesResponse{
				Groups:    []*GroupStateDesc{&gs1, &gs3},
				NextToken: "",
			},
			maxRuleGroups: 2,
		},
		"With duplicate with a new newer rule evaluation - pagination": {
			input: []*RulesResponse{
				{
					Groups:    []*GroupStateDesc{&gs3},
					NextToken: GetRuleGroupNextToken(gs3.Group.Namespace, gs3.Group.Name),
				},
				{
					Groups:    []*GroupStateDesc{&gs1},
					NextToken: GetRuleGroupNextToken(gs1.Group.Namespace, gs1.Group.Name),
				},
				{
					Groups:    []*GroupStateDesc{&gs2},
					NextToken: GetRuleGroupNextToken(gs2.Group.Namespace, gs2.Group.Name),
				},
				{
					Groups:    []*GroupStateDesc{&gs1NotRun},
					NextToken: GetRuleGroupNextToken(gs1NotRun.Group.Namespace, gs1NotRun.Group.Name),
				},
			},
			expectedOutput: &RulesResponse{
				Groups:    []*GroupStateDesc{&gs1},
				NextToken: GetRuleGroupNextToken(gs1.Group.Namespace, gs1.Group.Name),
			},
			maxRuleGroups: 1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			out := mergeGroupStateDesc(tc.input, tc.maxRuleGroups, true)
			slices.SortFunc(out.Groups, func(a, b *GroupStateDesc) int {
				fileCompare := strings.Compare(a.Group.Namespace, b.Group.Namespace)
				if fileCompare != 0 {
					return fileCompare
				}
				return strings.Compare(a.Group.Name, b.Group.Name)
			})
			require.Equal(t, int(tc.maxRuleGroups), len(out.Groups))
			t.Log(tc.expectedOutput)
			t.Log(out)
			require.True(t, reflect.DeepEqual(tc.expectedOutput, out))
		})
	}

}
