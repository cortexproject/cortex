package ruler

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
)

func TestGetRuleGroupNextToken(t *testing.T) {
	t.Run("deterministic output", func(t *testing.T) {
		token1 := GetRuleGroupNextToken("namespace1", "group1")
		token2 := GetRuleGroupNextToken("namespace1", "group1")
		assert.Equal(t, token1, token2)
	})

	t.Run("different inputs produce different tokens", func(t *testing.T) {
		token1 := GetRuleGroupNextToken("namespace1", "group1")
		token2 := GetRuleGroupNextToken("namespace1", "group2")
		token3 := GetRuleGroupNextToken("namespace2", "group1")
		assert.NotEqual(t, token1, token2)
		assert.NotEqual(t, token1, token3)
		assert.NotEqual(t, token2, token3)
	})

	t.Run("hex encoded sha1", func(t *testing.T) {
		token := GetRuleGroupNextToken("ns", "grp")
		assert.Len(t, token, 40) // SHA1 hex = 40 chars
	})
}

func TestPaginatedGroupStatesSort(t *testing.T) {
	groups := PaginatedGroupStates{
		{Group: &rulespb.RuleGroupDesc{Namespace: "z-namespace", Name: "z-group"}},
		{Group: &rulespb.RuleGroupDesc{Namespace: "a-namespace", Name: "a-group"}},
		{Group: &rulespb.RuleGroupDesc{Namespace: "m-namespace", Name: "m-group"}},
	}

	sort.Sort(groups)

	// Verify sorted by token order
	for i := 0; i < len(groups)-1; i++ {
		tokenI := GetRuleGroupNextToken(groups[i].Group.Namespace, groups[i].Group.Name)
		tokenJ := GetRuleGroupNextToken(groups[i+1].Group.Namespace, groups[i+1].Group.Name)
		assert.Less(t, tokenI, tokenJ)
	}
}

func TestGeneratePage(t *testing.T) {
	groups := make([]*GroupStateDesc, 5)
	for i := range groups {
		groups[i] = &GroupStateDesc{
			Group: &rulespb.RuleGroupDesc{
				Namespace: "namespace",
				Name:      string(rune('a' + i)),
			},
		}
	}

	t.Run("returns all groups when maxRuleGroups exceeds total", func(t *testing.T) {
		result, token := generatePage(groups, 10, 0)
		assert.Len(t, result, 5)
		assert.Empty(t, token)
	})

	t.Run("returns all groups when maxRuleGroups equals total", func(t *testing.T) {
		result, token := generatePage(groups, 5, 0)
		assert.Len(t, result, 5)
		assert.Empty(t, token)
	})

	t.Run("returns page with next token when more groups exist", func(t *testing.T) {
		result, token := generatePage(groups, 3, 0)
		require.Len(t, result, 3)
		assert.NotEmpty(t, token)
		expectedToken := GetRuleGroupNextToken(result[2].Group.Namespace, result[2].Group.Name)
		assert.Equal(t, expectedToken, token)
	})

	t.Run("returns all groups when maxRuleGroups is negative", func(t *testing.T) {
		result, token := generatePage(groups, -1, 0)
		assert.Len(t, result, 5)
		assert.Empty(t, token)
	})

	t.Run("empty input", func(t *testing.T) {
		result, token := generatePage(nil, 10, 0)
		assert.Empty(t, result)
		assert.Empty(t, token)
	})

	t.Run("page of one", func(t *testing.T) {
		result, token := generatePage(groups, 1, 0)
		require.Len(t, result, 1)
		assert.NotEmpty(t, token)
	})
}

// groupWithRules builds a group holding ruleCount active rules. generatePage only
// looks at how many active rules a group has, not at their contents.
func groupWithRules(name string, ruleCount int) *GroupStateDesc {
	activeRules := make([]*RuleStateDesc, ruleCount)
	for i := range activeRules {
		activeRules[i] = &RuleStateDesc{Rule: &rulespb.RuleDesc{Expr: "up"}}
	}
	return &GroupStateDesc{
		Group:       &rulespb.RuleGroupDesc{Namespace: "namespace", Name: name},
		ActiveRules: activeRules,
	}
}

func TestGeneratePage_MaxRules(t *testing.T) {
	// ruleCounts describes the input groups, which generatePage expects to already
	// be sorted. Group names ascend with the slice index so that the input order is
	// also token order, letting the cases below assert on the surviving prefix.
	for _, tc := range []struct {
		name           string
		ruleCounts     []int
		maxRuleGroups  int
		maxRules       uint
		expectedGroups int
		expectedToken  int // index into ruleCounts of the group the token points at, or -1 for no token
	}{
		{
			name:           "maxRules disabled returns every group",
			ruleCounts:     []int{2, 2, 2},
			maxRuleGroups:  0,
			maxRules:       0,
			expectedGroups: 3,
			expectedToken:  -1,
		},
		{
			name:           "cumulative rule count below maxRules",
			ruleCounts:     []int{2, 2, 2},
			maxRuleGroups:  0,
			maxRules:       10,
			expectedGroups: 3,
			expectedToken:  -1,
		},
		{
			name:           "cumulative rule count exactly at maxRules is not truncated",
			ruleCounts:     []int{2, 2, 2},
			maxRuleGroups:  0,
			maxRules:       6,
			expectedGroups: 3,
			expectedToken:  -1,
		},
		{
			name:           "maxRules truncates part way through the list",
			ruleCounts:     []int{2, 2, 2},
			maxRuleGroups:  0,
			maxRules:       5,
			expectedGroups: 2,
			expectedToken:  1,
		},
		{
			// A rule group is indivisible, so a group larger than maxRules is returned
			// whole rather than dropped. It is the only group, so there is nothing left
			// to page to and the token must be empty.
			name:           "lone group larger than maxRules is returned without a token",
			ruleCounts:     []int{5},
			maxRuleGroups:  0,
			maxRules:       2,
			expectedGroups: 1,
			expectedToken:  -1,
		},
		{
			name:           "oversized first group still yields a token when more groups follow",
			ruleCounts:     []int{5, 1},
			maxRuleGroups:  0,
			maxRules:       2,
			expectedGroups: 1,
			expectedToken:  0,
		},
		{
			name:           "maxRuleGroups binds before maxRules",
			ruleCounts:     []int{1, 1, 1},
			maxRuleGroups:  2,
			maxRules:       100,
			expectedGroups: 2,
			expectedToken:  1,
		},
		{
			name:           "maxRules binds before maxRuleGroups",
			ruleCounts:     []int{2, 2, 2},
			maxRuleGroups:  3,
			maxRules:       3,
			expectedGroups: 1,
			expectedToken:  0,
		},
		{
			name:           "groups without active rules are never limited by maxRules",
			ruleCounts:     []int{0, 0, 0},
			maxRuleGroups:  0,
			maxRules:       1,
			expectedGroups: 3,
			expectedToken:  -1,
		},
		{
			name:           "empty input with maxRules set",
			ruleCounts:     nil,
			maxRuleGroups:  0,
			maxRules:       5,
			expectedGroups: 0,
			expectedToken:  -1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups := make([]*GroupStateDesc, len(tc.ruleCounts))
			for i, count := range tc.ruleCounts {
				groups[i] = groupWithRules(string(rune('a'+i)), count)
			}

			result, token := generatePage(groups, tc.maxRuleGroups, tc.maxRules)

			require.Len(t, result, tc.expectedGroups)
			// The page must be the leading prefix of the input.
			for i := range result {
				assert.Same(t, groups[i], result[i])
			}

			if tc.expectedToken < 0 {
				assert.Empty(t, token)
				return
			}
			expected := groups[tc.expectedToken]
			assert.Equal(t, GetRuleGroupNextToken(expected.Group.Namespace, expected.Group.Name), token)
		})
	}
}
