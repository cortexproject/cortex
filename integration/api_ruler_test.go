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

	c, err := e2ecortex.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	namespace := "test_namespace"
	rg := rulefmt.RuleGroup{
		Name:     "test_group",
		Interval: 100,
		Rules: []rulefmt.Rule{
			rulefmt.Rule{
				Record: "test_rule",
				Expr:   "up",
			},
		},
	}

	require.NoError(t, c.SetRuleGroup(rg, namespace))

	rgs, err := c.GetRuleGroups()
	require.NoError(t, err)

	retrievedNamespace, exists := rgs[namespace]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, rg.Name)

	// Ensure the rule group is loaded by the per-tenant Prometheus rules manager
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))
	require.NoError(t, c.DeleteRuleGroup(namespace, rg.Name))
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(0), "cortex_ruler_managers_total"))

	_, err = c.GetRuleGroups()
	require.Error(t, err)
}
