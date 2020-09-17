// +build requires_docker

package integration

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestRulerAPI(t *testing.T) {
	var (
		namespaceOne = "test_/encoded_+namespace/?"
		namespaceTwo = "test_/encoded_+namespace/?/two"
	)
	ruleGroup := createTestRuleGroup(t)

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

	// Set the rule group into the ruler
	require.NoError(t, c.SetRuleGroup(ruleGroup, namespaceOne))

	// Wait until the user manager is created
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))

	// Check to ensure the rules running in the ruler match what was set
	rgs, err := c.GetRuleGroups()
	require.NoError(t, err)

	retrievedNamespace, exists := rgs[namespaceOne]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, ruleGroup.Name)

	// Add a second rule group with a similar namespace
	require.NoError(t, c.SetRuleGroup(ruleGroup, namespaceTwo))
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(2), "cortex_prometheus_rule_group_rules"))

	// Check to ensure the rules running in the ruler match what was set
	rgs, err = c.GetRuleGroups()
	require.NoError(t, err)

	retrievedNamespace, exists = rgs[namespaceOne]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, ruleGroup.Name)

	retrievedNamespace, exists = rgs[namespaceTwo]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, ruleGroup.Name)

	// Delete the set rule groups
	require.NoError(t, c.DeleteRuleGroup(namespaceOne, ruleGroup.Name))
	require.NoError(t, c.DeleteRuleNamespace(namespaceTwo))

	// Wait until the users manager has been terminated
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(0), "cortex_ruler_managers_total"))

	// Check to ensure the rule groups are no longer active
	_, err = c.GetRuleGroups()
	require.Error(t, err)

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Ruler, ruler)
}

func TestRulerAPISingleBinary(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	namespace := "ns"
	user := "fake"

	configOverrides := map[string]string{
		"-ruler.storage.local.directory": filepath.Join(e2e.ContainerSharedDir, "ruler_configs"),
		"-ruler.poll-interval":           "2s",
		"-ruler.rule-path":               filepath.Join(e2e.ContainerSharedDir, "rule_tmp/"),
	}

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config.yaml", cortexConfigFile))
	require.NoError(t, writeFileToSharedDir(s, filepath.Join("ruler_configs", user, namespace), []byte(cortexRulerUserConfigYaml)))
	cortex := e2ecortex.NewSingleBinaryWithConfigFile("cortex", cortexConfigFile, configOverrides, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Create a client with the ruler address configured
	c, err := e2ecortex.NewClient("", "", "", cortex.HTTPEndpoint(), "")
	require.NoError(t, err)

	// Wait until the user manager is created
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))

	// Check to ensure the rules running in the cortex match what was set
	rgs, err := c.GetRuleGroups()
	require.NoError(t, err)

	retrievedNamespace, exists := rgs[namespace]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, "rule")

	// Check to make sure prometheus engine metrics are available for both engine types
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"prometheus_engine_queries"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "engine", "querier"))))

	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"prometheus_engine_queries"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "engine", "ruler"))))

	// Test Cleanup and Restart

	// Stop the running cortex
	require.NoError(t, cortex.Stop())

	// Restart Cortex with identical configs
	cortexRestarted := e2ecortex.NewSingleBinaryWithConfigFile("cortex-restarted", cortexConfigFile, configOverrides, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortexRestarted))

	// Wait until the user manager is created
	require.NoError(t, cortexRestarted.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))
}

func TestRulerAlertmanager(t *testing.T) {
	var namespaceOne = "test_/encoded_+namespace/?"
	ruleGroup := createTestRuleGroup(t)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	minio := e2edb.NewMinio(9000, RulerConfigs["-ruler.storage.s3.buckets"])
	require.NoError(t, s.StartAndWaitReady(minio, dynamo))

	// Have at least one alertmanager configuration.
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(cortexAlertmanagerUserConfigYaml)))

	// Start Alertmanagers.
	am1 := e2ecortex.NewAlertmanager("alertmanager1", mergeFlags(AlertmanagerFlags, AlertmanagerLocalFlags), "")
	require.NoError(t, s.StartAndWaitReady(am1))
	am2 := e2ecortex.NewAlertmanager("alertmanager2", mergeFlags(AlertmanagerFlags, AlertmanagerLocalFlags), "")
	require.NoError(t, s.StartAndWaitReady(am2))

	am1URL := "http://" + am1.HTTPEndpoint()
	am2URL := "http://" + am2.HTTPEndpoint()

	// Connect the ruler to Alertmanagers
	configOverrides := map[string]string{
		"-ruler.alertmanager-url": strings.Join([]string{am1URL, am2URL}, ","),
	}

	// Start Ruler.
	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))
	ruler := e2ecortex.NewRuler("ruler", mergeFlags(ChunksStorageFlags, RulerConfigs, configOverrides), "")
	require.NoError(t, s.StartAndWaitReady(ruler))

	// Create a client with the ruler address configured
	c, err := e2ecortex.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	// Set the rule group into the ruler
	require.NoError(t, c.SetRuleGroup(ruleGroup, namespaceOne))

	// Wait until the user manager is created
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))

	//  Wait until we've discovered the alertmanagers.
	require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_prometheus_notifications_alertmanagers_discovered"}, e2e.WaitMissingMetrics))
}

func createTestRuleGroup(t *testing.T) rulefmt.RuleGroup {
	t.Helper()

	var (
		recordNode = yaml.Node{}
		exprNode   = yaml.Node{}
	)

	recordNode.SetString("test_rule")
	exprNode.SetString("up")
	return rulefmt.RuleGroup{
		Name:     "test_encoded_+\"+group_name/?",
		Interval: 100,
		Rules: []rulefmt.RuleNode{
			{
				Record: recordNode,
				Expr:   exprNode,
			},
		},
	}
}
