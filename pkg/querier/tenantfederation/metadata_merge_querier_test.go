package tenantfederation

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/tenant"
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

func (m *mockMetadataQuerier) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
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
					{Metric: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				},
			},
			orgId: "user-1",
			expectedResults: []scrape.MetricMetadata{
				{Metric: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
			},
			expectedMetrics: expectedSingleTenantsMetadataMetrics,
		},
		{
			name: "should be merged two tenants results",
			tenantIdToMetadata: map[string][]scrape.MetricMetadata{
				"user-1": {
					{Metric: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				},
				"user-2": {
					{Metric: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
					{Metric: "metadata3", Help: "metadata3 help", Type: "gauge", Unit: ""},
				},
			},
			orgId: "user-1|user-2",
			expectedResults: []scrape.MetricMetadata{
				{Metric: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				{Metric: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
				{Metric: "metadata3", Help: "metadata3 help", Type: "gauge", Unit: ""},
			},
			expectedMetrics: expectedTwoTenantsMetadataMetrics,
		},
		{
			name: "should be deduplicated when the same metadata exist",
			tenantIdToMetadata: map[string][]scrape.MetricMetadata{
				"user-1": {
					{Metric: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
					{Metric: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
				},
				"user-2": {
					{Metric: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
				},
			},
			orgId: "user-1|user-2",
			expectedResults: []scrape.MetricMetadata{
				{Metric: "metadata1", Help: "metadata1 help", Type: "gauge", Unit: ""},
				{Metric: "metadata2", Help: "metadata2 help", Type: "counter", Unit: ""},
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
			metadata, err := mergeMetadataQuerier.MetricsMetadata(user.InjectOrgID(context.Background(), test.orgId))
			require.NoError(t, err)
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(test.expectedMetrics), "cortex_querier_federated_tenants_per_metadata_query"))
			require.Equal(t, test.expectedResults, metadata)
		})
	}
}
