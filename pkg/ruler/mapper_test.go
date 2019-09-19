package ruler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/spf13/afero"
)

var (
	testUser = "user1"

	initialRuleSet = map[string][]rulefmt.RuleGroup{
		"file_one": {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
		},
	}

	outOfOrderRuleSet = map[string][]rulefmt.RuleGroup{
		"file_one": {
			{
				Name: "rulegroup_two",
				Rules: []rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
		},
	}

	updatedRuleSet = map[string][]rulefmt.RuleGroup{
		"file_one": {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_three",
				Rules: []rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
		},
	}
)

func Test_mapper_MapRules(t *testing.T) {
	m := &mapper{
		Path: "/rules",
		FS:   afero.NewMemMapFs(),
	}

	t.Run("basic rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, "/rules/user1/file_one", files[0])
		require.NoError(t, err)
	})

	t.Run("identical rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.False(t, updated)
		require.Len(t, files, 0)
		require.NoError(t, err)
	})

	t.Run("out of order identical rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, outOfOrderRuleSet)
		require.False(t, updated)
		require.Len(t, files, 0)
		require.NoError(t, err)

	})

	t.Run("updated rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, updatedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, "/rules/user1/file_one", files[0])
		require.NoError(t, err)
	})
}
