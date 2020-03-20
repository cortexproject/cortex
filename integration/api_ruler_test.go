// +build requires_docker

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	rulefmt "github.com/cortexproject/cortex/pkg/ruler/legacy_rulefmt"
)

func TestRulerAPI(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	minio := e2edb.NewMinio(9000, RulerConfigs["-ruler.storage.s3.buckets"])
	require.NoError(t, s.StartAndWaitReady(minio, dynamo))

	// Start Cortex components.
	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))
	ruler := e2ecortex.NewRuler("ruler", mergeFlags(ChunksStorageFlags, RulerConfigs), "")
	require.NoError(t, s.StartAndWaitReady(ruler))

	// Create a client with the ruler address configured
	c, err := e2ecortex.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	// Create example namespace and rule group to use for tests, using strings that
	// require url escaping.
	namespace := "test_encoded_+namespace?"
	rg := rulefmt.RuleGroup{
		Name:     "test_encoded_+\"+group_name?",
		Interval: 100,
		Rules: []rulefmt.Rule{
			rulefmt.Rule{
				Record: "test_rule",
				Expr:   "up",
			},
		},
	}

	// Set the rule group into the ruler
	require.NoError(t, c.SetRuleGroup(rg, namespace))

	// Wait until the user manager is created
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))

	// Check to ensure the rules running in the ruler match what was set
	rgs, err := c.GetRuleGroups()
	require.NoError(t, err)

	retrievedNamespace, exists := rgs[namespace]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, rg.Name)

	// Delete the set rule group
	require.NoError(t, c.DeleteRuleGroup(namespace, rg.Name))

	// Wait until the users manager has been terminated
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(0), "cortex_ruler_managers_total"))

	// Check to ensure the rule groups are no longer active
	_, err = c.GetRuleGroups()
	require.Error(t, err)

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Ruler, ruler)
}
