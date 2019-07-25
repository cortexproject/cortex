package clients

import (
	"context"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/clients/gcp"
	"github.com/cortexproject/cortex/pkg/storage/rules"
	"github.com/cortexproject/cortex/pkg/storage/testutils"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	userID    = "userID"
	namespace = "default"
)

var (
	exampleRuleGrp = rulefmt.RuleGroup{
		Name: "example_rulegroup_one",
	}
)

func TestRuleStoreBasic(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client rules.RuleStore) {
		const batchSize = 5
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		err := client.SetRuleGroup(ctx, userID, namespace, exampleRuleGrp)
		require.NoError(t, err)

		rg, err := client.GetRuleGroup(ctx, userID, namespace, exampleRuleGrp.Name)
		require.NoError(t, err)
		assert.Equal(t, exampleRuleGrp.Name, rg.Name())

		err = client.DeleteRuleGroup(ctx, userID, namespace, exampleRuleGrp.Name)
		require.NoError(t, err)

		rg, err = client.GetRuleGroup(ctx, userID, namespace, exampleRuleGrp.Name)
		require.Error(t, err)
		assert.Nil(t, rg)
	})
}

func forAllFixtures(t *testing.T, clientTest func(*testing.T, rules.RuleStore)) {
	var fixtures []testutils.Fixture
	fixtures = append(fixtures, gcp.Fixtures...)

	for _, fixture := range fixtures {
		r, err := fixture.Clients()
		require.NoError(t, err)

		clientTest(t, r)
	}
}
