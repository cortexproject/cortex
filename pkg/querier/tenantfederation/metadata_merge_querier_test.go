package tenantfederation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

var (
	expectedSingleTenantsMetadataMetrics = `
# HELP cortex_querier_federated_tenants_per_metadata_query Number of tenants per metadata query.
# TYPE cortex_querier_federated_tenants_per_metadata_query histogram
cortex_querier_federated_tenants_per_metadata_query_bucket{le="1"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="2"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="4"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="8"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="16"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="32"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="64"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="+Inf"} 1
cortex_querier_federated_tenants_per_metadata_query_sum 1
cortex_querier_federated_tenants_per_metadata_query_count 1
`

	expectedTwoTenantsMetadataMetrics = `
# HELP cortex_querier_federated_tenants_per_metadata_query Number of tenants per metadata query.
# TYPE cortex_querier_federated_tenants_per_metadata_query histogram
cortex_querier_federated_tenants_per_metadata_query_bucket{le="1"} 0
cortex_querier_federated_tenants_per_metadata_query_bucket{le="2"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="4"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="8"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="16"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="32"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="64"} 1
cortex_querier_federated_tenants_per_metadata_query_bucket{le="+Inf"} 1
cortex_querier_federated_tenants_per_metadata_query_sum 2
cortex_querier_federated_tenants_per_metadata_query_count 1
`
)

type mockMetadataQuerier struct {
	tenantIdToMetadata map[string][]scrape.MetricMetadata
}

func (m *mockMetadataQuerier) MetricsMetadata(ctx context.Context, _ *client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error) {
	// Due to lint check for `ensure the query path is supporting multiple tenants`
	ids, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}

	id := ids[0]
	if res, ok := m.tenantIdToMetadata[id]; !ok {
		return nil, fmt.Errorf("tenant not found, tenantId: %s", id)
	} else {
		return res, nil
	}
}

func Test_mergeMetadataQuerier_MetricsMetadata(t *testing.T) {
	// set a multi tenant resolver
	tenant.WithDefaultResolver(tenant.NewMultiResolver())

	tests := []struct {
		name               string
		tenantIdToMetadata map[string][]scrape.MetricMetadata
		orgId              string
		expectedResults    []scrape.MetricMetadata
		expectedMetrics    string
	}{
		{
			name: "single tenant",
			tenantIdToMetadata: map[string][]scrape.MetricMetadata{
				"user-1": {
					{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				},
			},
			orgId: "user-1",
			expectedResults: []scrape.MetricMetadata{
				{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
			},
			expectedMetrics: expectedSingleTenantsMetadataMetrics,
		},
		{
			name: "should be merged two tenants results",
			tenantIdToMetadata: map[string][]scrape.MetricMetadata{
				"user-1": {
					{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				},
				"user-2": {
					{MetricFamily: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
					{MetricFamily: "metadata3", Help: "metadata3 help", Type: "gauge", Unit: ""},
				},
			},
			orgId: "user-1|user-2",
			expectedResults: []scrape.MetricMetadata{
				{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				{MetricFamily: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
				{MetricFamily: "metadata3", Help: "metadata3 help", Type: "gauge", Unit: ""},
			},
			expectedMetrics: expectedTwoTenantsMetadataMetrics,
		},
		{
			name: "should be deduplicated when the same metadata exist",
			tenantIdToMetadata: map[string][]scrape.MetricMetadata{
				"user-1": {
					{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
					{MetricFamily: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
				},
				"user-2": {
					{MetricFamily: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
				},
			},
			orgId: "user-1|user-2",
			expectedResults: []scrape.MetricMetadata{
				{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				{MetricFamily: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
			},
			expectedMetrics: expectedTwoTenantsMetadataMetrics,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			upstream := mockMetadataQuerier{
				tenantIdToMetadata: test.tenantIdToMetadata,
			}

			mergeMetadataQuerier := NewMetadataQuerier(&upstream, defaultMaxConcurrency, reg)
			metadata, err := mergeMetadataQuerier.MetricsMetadata(user.InjectOrgID(context.Background(), test.orgId), &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1, Metric: ""})
			require.NoError(t, err)
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(test.expectedMetrics), "cortex_querier_federated_tenants_per_metadata_query"))
			require.Equal(t, test.expectedResults, metadata)
		})
	}
}

func Test_mergeMetadataQuerier_MetricsMetadata_WhenUseRegexResolver(t *testing.T) {
	// set a regex tenant resolver
	reg := prometheus.NewRegistry()
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockIter("__markers__", []string{}, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-1"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath("user-2"), false, nil)
	bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath("user-2"), false, nil)

	bucketClientFactory := func(ctx context.Context) (objstore.InstrumentedBucket, error) {
		return bucketClient, nil
	}

	usersScannerConfig := cortex_tsdb.UsersScannerConfig{Strategy: cortex_tsdb.UserScanStrategyList}
	tenantFederationConfig := Config{UserSyncInterval: time.Second}
	regexResolver, err := NewRegexResolver(usersScannerConfig, tenantFederationConfig, reg, bucketClientFactory, log.NewNopLogger())
	require.NoError(t, err)
	tenant.WithDefaultResolver(regexResolver)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), regexResolver))

	// wait update knownUsers
	test.Poll(t, time.Second*10, true, func() interface{} {
		return testutil.ToFloat64(regexResolver.lastUpdateUserRun) > 0 && testutil.ToFloat64(regexResolver.discoveredUsers) == 2
	})

	tests := []struct {
		name               string
		tenantIdToMetadata map[string][]scrape.MetricMetadata
		orgId              string
		expectedResults    []scrape.MetricMetadata
		expectedMetrics    string
	}{
		{
			name: "single tenant",
			tenantIdToMetadata: map[string][]scrape.MetricMetadata{
				"user-1": {
					{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				},
			},
			orgId: "user-1",
			expectedResults: []scrape.MetricMetadata{
				{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
			},
			expectedMetrics: expectedSingleTenantsMetadataMetrics,
		},
		{
			name: "should be merged two tenants results",
			tenantIdToMetadata: map[string][]scrape.MetricMetadata{
				"user-1": {
					{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				},
				"user-2": {
					{MetricFamily: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
					{MetricFamily: "metadata3", Help: "metadata3 help", Type: "gauge", Unit: ""},
				},
			},
			orgId: "user-.+",
			expectedResults: []scrape.MetricMetadata{
				{MetricFamily: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				{MetricFamily: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
				{MetricFamily: "metadata3", Help: "metadata3 help", Type: "gauge", Unit: ""},
			},
			expectedMetrics: expectedTwoTenantsMetadataMetrics,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reg = prometheus.NewPedanticRegistry()
			upstream := mockMetadataQuerier{
				tenantIdToMetadata: test.tenantIdToMetadata,
			}

			mergeMetadataQuerier := NewMetadataQuerier(&upstream, defaultMaxConcurrency, reg)
			metadata, err := mergeMetadataQuerier.MetricsMetadata(user.InjectOrgID(context.Background(), test.orgId), &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1, Metric: ""})
			require.NoError(t, err)
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(test.expectedMetrics), "cortex_querier_federated_tenants_per_metadata_query"))
			require.Equal(t, test.expectedResults, metadata)
		})
	}
}
