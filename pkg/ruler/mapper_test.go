package ruler

import (
	"os"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	legacy_rulefmt "github.com/cortexproject/cortex/pkg/ruler/legacy_rulefmt"
)

var (
	testUser = "user1"

	initialRuleSet = map[string][]legacy_rulefmt.RuleGroup{
		"file_one": {
			{
				Name: "rulegroup_one",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
		},
	}

	outOfOrderRuleSet = map[string][]legacy_rulefmt.RuleGroup{
		"file_one": {
			{
				Name: "rulegroup_two",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_one",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
		},
	}

	updatedRuleSet = map[string][]legacy_rulefmt.RuleGroup{
		"file_one": {
			{
				Name: "rulegroup_one",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_three",
				Rules: []legacy_rulefmt.Rule{
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
	l := log.NewLogfmtLogger(os.Stdout)
	l = level.NewFilter(l, level.AllowInfo())
	m := &mapper{
		Path:   "/rules",
		FS:     afero.NewMemMapFs(),
		logger: l,
	}

	t.Run("basic rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, "/rules/user1/file_one", files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, "/rules/user1/file_one")
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("identical rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, "/rules/user1/file_one")
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("out of order identical rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, outOfOrderRuleSet)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, "/rules/user1/file_one")
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("updated rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, updatedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, "/rules/user1/file_one", files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, "/rules/user1/file_one")
		require.True(t, exists)
		require.NoError(t, err)
	})
}

var (
	twoFilesRuleSet = map[string][]legacy_rulefmt.RuleGroup{
		"file_one": {
			{
				Name: "rulegroup_one",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
		},
		"file_two": {
			{
				Name: "rulegroup_one",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
		},
	}

	twoFilesUpdatedRuleSet = map[string][]legacy_rulefmt.RuleGroup{
		"file_one": {
			{
				Name: "rulegroup_one",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
		},
		"file_two": {
			{
				Name: "rulegroup_one",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_ruleupdated",
						Expr:   "example_exprupdated",
					},
				},
			},
		},
	}

	twoFilesDeletedRuleSet = map[string][]legacy_rulefmt.RuleGroup{
		"file_one": {
			{
				Name: "rulegroup_one",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []legacy_rulefmt.Rule{
					{
						Record: "example_rule",
						Expr:   "example_expr",
					},
				},
			},
		},
	}
)

func Test_mapper_MapRulesMultipleFiles(t *testing.T) {
	l := log.NewLogfmtLogger(os.Stdout)
	l = level.NewFilter(l, level.AllowInfo())
	m := &mapper{
		Path:   "/rules",
		FS:     afero.NewMemMapFs(),
		logger: l,
	}

	t.Run("basic rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, "/rules/user1/file_one", files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, "/rules/user1/file_one")
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("add a file", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, twoFilesRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.True(t, sliceContains(t, "/rules/user1/file_one", files))
		require.True(t, sliceContains(t, "/rules/user1/file_two", files))
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, "/rules/user1/file_one")
		require.True(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, "/rules/user1/file_two")
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("update one file", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, twoFilesUpdatedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.True(t, sliceContains(t, "/rules/user1/file_one", files))
		require.True(t, sliceContains(t, "/rules/user1/file_two", files))
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, "/rules/user1/file_one")
		require.True(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, "/rules/user1/file_two")
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("delete one file", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, twoFilesDeletedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, "/rules/user1/file_one", files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, "/rules/user1/file_one")
		require.True(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, "/rules/user1/file_two")
		require.False(t, exists)
		require.NoError(t, err)
	})

}

func sliceContains(t *testing.T, find string, in []string) bool {
	t.Helper()

	for _, s := range in {
		if find == s {
			return true
		}
	}

	return false
}
