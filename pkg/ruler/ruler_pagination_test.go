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
		result, token := generatePage(groups, 10)
		assert.Len(t, result, 5)
		assert.Empty(t, token)
	})

	t.Run("returns all groups when maxRuleGroups equals total", func(t *testing.T) {
		result, token := generatePage(groups, 5)
		assert.Len(t, result, 5)
		assert.Empty(t, token)
	})

	t.Run("returns page with next token when more groups exist", func(t *testing.T) {
		result, token := generatePage(groups, 3)
		require.Len(t, result, 3)
		assert.NotEmpty(t, token)
		expectedToken := GetRuleGroupNextToken(result[2].Group.Namespace, result[2].Group.Name)
		assert.Equal(t, expectedToken, token)
	})

	t.Run("returns all groups when maxRuleGroups is negative", func(t *testing.T) {
		result, token := generatePage(groups, -1)
		assert.Len(t, result, 5)
		assert.Empty(t, token)
	})

	t.Run("empty input", func(t *testing.T) {
		result, token := generatePage(nil, 10)
		assert.Empty(t, result)
		assert.Empty(t, token)
	})

	t.Run("page of one", func(t *testing.T) {
		result, token := generatePage(groups, 1)
		require.Len(t, result, 1)
		assert.NotEmpty(t, token)
	})
}
