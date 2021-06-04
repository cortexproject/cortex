// +build requires_docker

package integration

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/integration/ca"
	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestRulerAPI(t *testing.T) {
	tests := map[string]struct {
		legacyRuleStore bool
	}{
		"objstore_rulestore": {},
		"legacy_rulestore": {
			legacyRuleStore: true,
		},
	}

	var (
		namespaceOne = "test_/encoded_+namespace/?"
		namespaceTwo = "test_/encoded_+namespace/?/two"
	)
	ruleGroup := createTestRuleGroup(t)

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Start dependencies.
			consul := e2edb.NewConsul()
			dynamo := e2edb.NewDynamoDB()
			minio := e2edb.NewMinio(9000, rulestoreBucketName)
			require.NoError(t, s.StartAndWaitReady(consul, minio, dynamo))

			// Start Cortex components.
			require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))
			ruler := e2ecortex.NewRuler("ruler", consul.NetworkHTTPEndpoint(), mergeFlags(ChunksStorageFlags(), RulerFlags(testCfg.legacyRuleStore)), "")
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

			// Test compression by inspecting the response Headers
			req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/api/prom/rules", ruler.HTTPEndpoint()), nil)
			require.NoError(t, err)

			req.Header.Set("X-Scope-OrgID", "user-1")
			req.Header.Set("Accept-Encoding", "gzip")

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Execute HTTP request
			res, err := http.DefaultClient.Do(req.WithContext(ctx))
			require.NoError(t, err)

			defer res.Body.Close()
			// We assert on the Vary header as the minimum response size for enabling compression is 1500 bytes.
			// This is enough to know whenever the handler for compression is enabled or not.
			require.Equal(t, "Accept-Encoding", res.Header.Get("Vary"))

			// Delete the set rule groups
			require.NoError(t, c.DeleteRuleGroup(namespaceOne, ruleGroup.Name))
			require.NoError(t, c.DeleteRuleNamespace(namespaceTwo))

			// Get the rule group and ensure it returns a 404
			resp, err := c.GetRuleGroup(namespaceOne, ruleGroup.Name)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusNotFound, resp.StatusCode)

			// Wait until the users manager has been terminated
			require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(0), "cortex_ruler_managers_total"))

			// Check to ensure the rule groups are no longer active
			_, err = c.GetRuleGroups()
			require.Error(t, err)

			// Ensure no service-specific metrics prefix is used by the wrong service.
			assertServiceMetricsPrefixes(t, Ruler, ruler)
		})
	}
}

func TestRulerAPISingleBinary(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	namespace := "ns"
	user := "fake"

	configOverrides := map[string]string{
		"-ruler-storage.local.directory": filepath.Join(e2e.ContainerSharedDir, "ruler_configs"),
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

func TestRulerEvaluationDelay(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	namespace := "ns"
	user := "fake"

	evaluationDelay := time.Minute * 5

	configOverrides := map[string]string{
		"-ruler-storage.local.directory":   filepath.Join(e2e.ContainerSharedDir, "ruler_configs"),
		"-ruler.poll-interval":             "2s",
		"-ruler.rule-path":                 filepath.Join(e2e.ContainerSharedDir, "rule_tmp/"),
		"-ruler.evaluation-delay-duration": evaluationDelay.String(),
	}

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config.yaml", cortexConfigFile))
	require.NoError(t, writeFileToSharedDir(s, filepath.Join("ruler_configs", user, namespace), []byte(cortexRulerEvalStaleNanConfigYaml)))
	cortex := e2ecortex.NewSingleBinaryWithConfigFile("cortex", cortexConfigFile, configOverrides, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Create a client with the ruler address configured
	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", cortex.HTTPEndpoint(), "")
	require.NoError(t, err)

	now := time.Now()

	// Generate series that includes stale nans
	var samplesToSend int = 10
	series := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "a_sometimes_stale_nan_series"},
			{Name: "instance", Value: "sometimes-stale"},
		},
	}
	series.Samples = make([]prompb.Sample, samplesToSend)
	posStale := 2

	// Create samples, that are delayed by the evaluation delay with increasing values.
	for pos := range series.Samples {
		series.Samples[pos].Timestamp = e2e.TimeToMilliseconds(now.Add(-evaluationDelay).Add(time.Duration(pos) * time.Second))
		series.Samples[pos].Value = float64(pos + 1)

		// insert staleness marker at the positions marked by posStale
		if pos == posStale {
			series.Samples[pos].Value = math.Float64frombits(value.StaleNaN)
		}
	}

	// Insert metrics
	res, err := c.Push([]prompb.TimeSeries{series})
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Get number of rule evaluations just after push
	ruleEvaluationsAfterPush, err := cortex.SumMetrics([]string{"cortex_prometheus_rule_evaluations_total"})
	require.NoError(t, err)

	// Wait until the rule is evaluated for the first time
	require.NoError(t, cortex.WaitSumMetrics(e2e.Greater(ruleEvaluationsAfterPush[0]), "cortex_prometheus_rule_evaluations_total"))

	// Query the timestamp of the latest result to ensure the evaluation is delayed
	result, err := c.Query("timestamp(stale_nan_eval)", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())

	vector := result.(model.Vector)
	require.Equal(t, 1, vector.Len(), "expect one sample returned")

	// 290 seconds gives 10 seconds of slack between the rule evaluation and the query
	// to account for CI latency, but ensures the latest evaluation was in the past.
	var maxDiff int64 = 290_000
	require.GreaterOrEqual(t, e2e.TimeToMilliseconds(time.Now())-int64(vector[0].Value)*1000, maxDiff)

	// Wait until all the pushed samples have been evaluated by the rule. This
	// ensures that rule results are successfully written even after a
	// staleness period.
	require.NoError(t, cortex.WaitSumMetrics(e2e.GreaterOrEqual(ruleEvaluationsAfterPush[0]+float64(samplesToSend)), "cortex_prometheus_rule_evaluations_total"))

	// query all results to verify rules have been evaluated correctly
	result, err = c.QueryRange("stale_nan_eval", now.Add(-evaluationDelay), now, time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, result.Type())

	matrix := result.(model.Matrix)
	require.GreaterOrEqual(t, 1, matrix.Len(), "expect at least a series returned")

	// Iterate through the values recorded and ensure they exist as expected.
	inputPos := 0
	for _, m := range matrix {
		for _, v := range m.Values {
			// Skip values for stale positions
			if inputPos == posStale {
				inputPos++
			}

			expectedValue := model.SampleValue(2 * (inputPos + 1))
			require.Equal(t, expectedValue, v.Value)

			// Look for next value
			inputPos++

			// We have found all input values
			if inputPos >= len(series.Samples) {
				break
			}
		}
	}
	require.Equal(t, len(series.Samples), inputPos, "expect to have returned all evaluations")
}

func TestRulerSharding(t *testing.T) {
	const numRulesGroups = 100

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Generate multiple rule groups, with 1 rule each.
	ruleGroups := make([]rulefmt.RuleGroup, numRulesGroups)
	expectedNames := make([]string, numRulesGroups)
	for i := 0; i < numRulesGroups; i++ {
		var recordNode yaml.Node
		var exprNode yaml.Node

		recordNode.SetString(fmt.Sprintf("rule_%d", i))
		exprNode.SetString(strconv.Itoa(i))
		ruleName := fmt.Sprintf("test_%d", i)

		expectedNames[i] = ruleName
		ruleGroups[i] = rulefmt.RuleGroup{
			Name:     ruleName,
			Interval: 60,
			Rules: []rulefmt.RuleNode{{
				Record: recordNode,
				Expr:   exprNode,
			}},
		}
	}

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, rulestoreBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Configure the ruler.
	rulerFlags := mergeFlags(
		BlocksStorageFlags(),
		RulerFlags(false),
		RulerShardingFlags(consul.NetworkHTTPEndpoint()),
		map[string]string{
			// Since we're not going to run any rule, we don't need the
			// store-gateway to be configured to a valid address.
			"-querier.store-gateway-addresses": "localhost:12345",
			// Enable the bucket index so we can skip the initial bucket scan.
			"-blocks-storage.bucket-store.bucket-index.enabled": "true",
		},
	)

	// Start rulers.
	ruler1 := e2ecortex.NewRuler("ruler-1", consul.NetworkHTTPEndpoint(), rulerFlags, "")
	ruler2 := e2ecortex.NewRuler("ruler-2", consul.NetworkHTTPEndpoint(), rulerFlags, "")
	rulers := e2ecortex.NewCompositeCortexService(ruler1, ruler2)
	require.NoError(t, s.StartAndWaitReady(ruler1, ruler2))

	// Upload rule groups to one of the rulers.
	c, err := e2ecortex.NewClient("", "", "", ruler1.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	for _, ruleGroup := range ruleGroups {
		require.NoError(t, c.SetRuleGroup(ruleGroup, "test"))
	}

	// Wait until rulers have loaded all rules.
	require.NoError(t, rulers.WaitSumMetricsWithOptions(e2e.Equals(numRulesGroups), []string{"cortex_prometheus_rule_group_rules"}, e2e.WaitMissingMetrics))

	// Since rulers have loaded all rules, we expect that rules have been sharded
	// between the two rulers.
	require.NoError(t, ruler1.WaitSumMetrics(e2e.Less(numRulesGroups), "cortex_prometheus_rule_group_rules"))
	require.NoError(t, ruler2.WaitSumMetrics(e2e.Less(numRulesGroups), "cortex_prometheus_rule_group_rules"))

	// Fetch the rules and ensure they match the configured ones.
	actualGroups, err := c.GetPrometheusRules()
	require.NoError(t, err)

	var actualNames []string
	for _, group := range actualGroups {
		actualNames = append(actualNames, group.Name)
	}
	assert.ElementsMatch(t, expectedNames, actualNames)
}

func TestRulerAlertmanager(t *testing.T) {
	var namespaceOne = "test_/encoded_+namespace/?"
	ruleGroup := createTestRuleGroup(t)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	dynamo := e2edb.NewDynamoDB()
	minio := e2edb.NewMinio(9000, rulestoreBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio, dynamo))

	// Have at least one alertmanager configuration.
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(cortexAlertmanagerUserConfigYaml)))

	// Start Alertmanagers.
	amFlags := mergeFlags(AlertmanagerFlags(), AlertmanagerLocalFlags())
	am1 := e2ecortex.NewAlertmanager("alertmanager1", amFlags, "")
	am2 := e2ecortex.NewAlertmanager("alertmanager2", amFlags, "")
	require.NoError(t, s.StartAndWaitReady(am1, am2))

	am1URL := "http://" + am1.HTTPEndpoint()
	am2URL := "http://" + am2.HTTPEndpoint()

	// Connect the ruler to Alertmanagers
	configOverrides := map[string]string{
		"-ruler.alertmanager-url": strings.Join([]string{am1URL, am2URL}, ","),
	}

	// Start Ruler.
	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))
	ruler := e2ecortex.NewRuler("ruler", consul.NetworkHTTPEndpoint(), mergeFlags(ChunksStorageFlags(), RulerFlags(false), configOverrides), "")
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

func TestRulerAlertmanagerTLS(t *testing.T) {
	var namespaceOne = "test_/encoded_+namespace/?"
	ruleGroup := createTestRuleGroup(t)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	dynamo := e2edb.NewDynamoDB()
	minio := e2edb.NewMinio(9000, rulestoreBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio, dynamo))

	// set the ca
	cert := ca.New("Ruler/Alertmanager Test")

	// Ensure the entire path of directories exist.
	require.NoError(t, os.MkdirAll(filepath.Join(s.SharedDir(), "certs"), os.ModePerm))

	require.NoError(t, cert.WriteCACertificate(filepath.Join(s.SharedDir(), caCertFile)))

	// server certificate
	require.NoError(t, cert.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "client"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		filepath.Join(s.SharedDir(), clientCertFile),
		filepath.Join(s.SharedDir(), clientKeyFile),
	))
	require.NoError(t, cert.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "server"},
			DNSNames:    []string{"ruler.alertmanager-client"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		},
		filepath.Join(s.SharedDir(), serverCertFile),
		filepath.Join(s.SharedDir(), serverKeyFile),
	))

	// Have at least one alertmanager configuration.
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(cortexAlertmanagerUserConfigYaml)))

	// Start Alertmanagers.
	amFlags := mergeFlags(
		AlertmanagerFlags(),
		AlertmanagerLocalFlags(),
		getServerHTTPTLSFlags(),
	)
	am1 := e2ecortex.NewAlertmanagerWithTLS("alertmanager1", amFlags, "")
	require.NoError(t, s.StartAndWaitReady(am1))

	// Connect the ruler to the Alertmanager
	configOverrides := mergeFlags(
		map[string]string{
			"-ruler.alertmanager-url": "https://" + am1.HTTPEndpoint(),
		},
		getTLSFlagsWithPrefix("ruler.alertmanager-client", "alertmanager", true),
	)

	// Start Ruler.
	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))
	ruler := e2ecortex.NewRuler("ruler", consul.NetworkHTTPEndpoint(), mergeFlags(ChunksStorageFlags(), RulerFlags(false), configOverrides), "")
	require.NoError(t, s.StartAndWaitReady(ruler))

	// Create a client with the ruler address configured
	c, err := e2ecortex.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	// Set the rule group into the ruler
	require.NoError(t, c.SetRuleGroup(ruleGroup, namespaceOne))

	// Wait until the user manager is created
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))

	//  Wait until we've discovered the alertmanagers.
	require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_prometheus_notifications_alertmanagers_discovered"}, e2e.WaitMissingMetrics))
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
