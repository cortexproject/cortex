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
		input          []*GroupStateDesc
		expectedOutput []*GroupStateDesc
	}

	testCases := map[string]testCase{
		"No duplicate": {
			input:          []*GroupStateDesc{&gs1, &gs2},
			expectedOutput: []*GroupStateDesc{&gs1, &gs2},
		},
		"No duplicate but not evaluated": {
			input:          []*GroupStateDesc{&gs1NotRun, &gs2NotRun},
			expectedOutput: []*GroupStateDesc{&gs1NotRun, &gs2NotRun},
		},
		"With exact duplicate": {
			input:          []*GroupStateDesc{&gs1, &gs2NotRun, &gs1, &gs2NotRun},
			expectedOutput: []*GroupStateDesc{&gs1, &gs2NotRun},
		},
		"With duplicates that are not evaluated": {
			input:          []*GroupStateDesc{&gs1, &gs2, &gs1NotRun, &gs2NotRun},
			expectedOutput: []*GroupStateDesc{&gs1, &gs2},
		},
		"With duplicate with a new newer rule evaluation": {
			input:          []*GroupStateDesc{&gs3, &gs1, &gs2, &gs1NotRun},
			expectedOutput: []*GroupStateDesc{&gs1, &gs3},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			out := mergeGroupStateDesc(tc.input)
			slices.SortFunc(out, func(a, b *GroupStateDesc) int {
				fileCompare := strings.Compare(a.Group.Namespace, b.Group.Namespace)
				if fileCompare != 0 {
					return fileCompare
				}
				return strings.Compare(a.Group.Name, b.Group.Name)
			})
			require.Equal(t, len(tc.expectedOutput), len(out))
			require.True(t, reflect.DeepEqual(tc.expectedOutput, out))
		})
	}

}
