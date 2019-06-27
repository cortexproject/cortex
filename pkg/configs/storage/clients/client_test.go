package clients

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/configs/storage/clients/gcp"
	"github.com/cortexproject/cortex/pkg/configs/storage/testutils"
	"github.com/prometheus/prometheus/pkg/rulefmt"
)

const (
	userID    = "userID"
	namespace = "default"
)

var (
	exampleRGOne = rulefmt.RuleGroup{
		Name: "example_rulegroup_one",
	}
	exampleRGTwo = rulefmt.RuleGroup{
		Name: "example_rulegroup_two",
	}
)

func TestRuleStoreBasic(t *testing.T) {
	forAllFixtures(t, func(t *testing.T, client configs.ConfigStore) {
		const batchSize = 5
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		err := client.SetRuleGroup(ctx, userID, namespace, exampleRGOne)
		assert.NoError(t, err)

		rg, err := client.GetRuleGroup(ctx, userID, namespace, exampleRGOne.Name)
		assert.NoError(t, err)
		assert.Equal(t, exampleRGOne.Name, rg.Name)
	})
}

func forAllFixtures(t *testing.T, f func(t *testing.T, client configs.ConfigStore)) {
	var fixtures []testutils.Fixture
	fixtures = append(fixtures, gcp.Fixtures...)
}
