package local

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	rulefmt "github.com/cortexproject/cortex/pkg/ruler/legacy_rulefmt"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestLoadingRules(t *testing.T) {
	user := "user"
	namespace := "ns"

	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ruleGroups := rulefmt.RuleGroups{
		Groups: []rulefmt.RuleGroup{
			{
				Name:     "rule",
				Interval: model.Duration(100 * time.Second),
				Rules: []rulefmt.Rule{
					{
						Record: "test_rule",
						Expr:   "up",
					},
				},
			},
		},
	}

	b, err := yaml.Marshal(ruleGroups)
	require.NoError(t, err)

	err = os.MkdirAll(path.Join(dir, user), 0777)
	require.NoError(t, err)

	err = ioutil.WriteFile(path.Join(dir, user, namespace), b, 0777)
	require.NoError(t, err)

	client, err := NewLocalRulesClient(Config{
		Directory: dir,
	})
	require.NoError(t, err)

	ctx := context.Background()
	userMap, err := client.ListAllRuleGroups(ctx)
	require.NoError(t, err)

	actual, found := userMap[user]
	require.True(t, found)

	require.Equal(t, len(ruleGroups.Groups), len(actual))
	for i, actualGroup := range actual {
		expected := rules.ToProto(user, namespace, ruleGroups.Groups[i])

		require.Equal(t, expected, actualGroup)
	}
}
