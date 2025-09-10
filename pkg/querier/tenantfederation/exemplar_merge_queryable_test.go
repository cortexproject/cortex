package tenantfederation

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

var (
	expectedSingleTenantsExemplarMetrics = `
# HELP cortex_querier_federated_tenants_per_exemplar_query Number of tenants per exemplar query.
# TYPE cortex_querier_federated_tenants_per_exemplar_query histogram
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="1"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="2"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="4"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="8"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="16"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="32"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="64"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="+Inf"} 1
cortex_querier_federated_tenants_per_exemplar_query_sum 1
cortex_querier_federated_tenants_per_exemplar_query_count 1
`

	expectedTwoTenantsExemplarMetrics = `
# HELP cortex_querier_federated_tenants_per_exemplar_query Number of tenants per exemplar query.
# TYPE cortex_querier_federated_tenants_per_exemplar_query histogram
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="1"} 0
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="2"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="4"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="8"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="16"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="32"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="64"} 1
cortex_querier_federated_tenants_per_exemplar_query_bucket{le="+Inf"} 1
cortex_querier_federated_tenants_per_exemplar_query_sum 2
cortex_querier_federated_tenants_per_exemplar_query_count 1
`
)

type mockExemplarQueryable struct {
	exemplarQueriers map[string]storage.ExemplarQuerier
}

func (m *mockExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	// Due to lint check for `ensure the query path is supporting multiple tenants`
	ids, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}

	id := ids[0]
	if _, ok := m.exemplarQueriers[id]; ok {
		return m.exemplarQueriers[id], nil
	} else {
		return nil, errors.New("failed to get exemplar querier")
	}
}

type mockExemplarQuerier struct {
	res []exemplar.QueryResult
	err error
}

func (m *mockExemplarQuerier) Select(_, _ int64, _ ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	if m.err != nil {
		return nil, m.err
	}

	return m.res, nil
}

// getFixtureExemplarResult1 returns fixture examplar1
func getFixtureExemplarResult1() []exemplar.QueryResult {
	res := []exemplar.QueryResult{
		{
			SeriesLabels: labels.FromStrings("__name__", "exemplar_series"),
			Exemplars: []exemplar.Exemplar{
				{
					Labels: labels.FromStrings("traceID", "123"),
					Value:  123,
					Ts:     1734942337900,
				},
			},
		},
	}
	return res
}

// getFixtureExemplarResult2 returns fixture examplar
func getFixtureExemplarResult2() []exemplar.QueryResult {
	res := []exemplar.QueryResult{
		{
			SeriesLabels: labels.FromStrings("__name__", "exemplar_series"),
			Exemplars: []exemplar.Exemplar{
				{
					Labels: labels.FromStrings("traceID", "456"),
					Value:  456,
					Ts:     1734942338000,
				},
			},
		},
	}
	return res
}

func Test_MergeExemplarQuerier_Select(t *testing.T) {
	// set a multi tenant resolver
	tenant.WithDefaultResolver(tenant.NewMultiResolver())

	tests := []struct {
		name            string
		upstream        mockExemplarQueryable
		matcher         [][]*labels.Matcher
		orgId           string
		expectedResult  []exemplar.QueryResult
		expectedErr     error
		expectedMetrics string
	}{
		{
			name: "should be treated as single tenant",
			upstream: mockExemplarQueryable{exemplarQueriers: map[string]storage.ExemplarQuerier{
				"user-1": &mockExemplarQuerier{res: getFixtureExemplarResult1()},
				"user-2": &mockExemplarQuerier{res: getFixtureExemplarResult2()},
			}},
			matcher: [][]*labels.Matcher{{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "exemplar_series"),
			}},
			orgId: "user-1",
			expectedResult: []exemplar.QueryResult{
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "123"),
							Value:  123,
							Ts:     1734942337900,
						},
					},
				},
			},
			expectedMetrics: expectedSingleTenantsExemplarMetrics,
		},
		{
			name: "two tenants results should be aggregated",
			upstream: mockExemplarQueryable{exemplarQueriers: map[string]storage.ExemplarQuerier{
				"user-1": &mockExemplarQuerier{res: getFixtureExemplarResult1()},
				"user-2": &mockExemplarQuerier{res: getFixtureExemplarResult2()},
			}},
			matcher: [][]*labels.Matcher{{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "exemplar_series"),
			}},
			orgId: "user-1|user-2",
			expectedResult: []exemplar.QueryResult{
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-1"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "123"),
							Value:  123,
							Ts:     1734942337900,
						},
					},
				},
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-2"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "456"),
							Value:  456,
							Ts:     1734942338000,
						},
					},
				},
			},
			expectedMetrics: expectedTwoTenantsExemplarMetrics,
		},
		{
			name: "should return the matched tenant query results",
			upstream: mockExemplarQueryable{exemplarQueriers: map[string]storage.ExemplarQuerier{
				"user-1": &mockExemplarQuerier{res: getFixtureExemplarResult1()},
				"user-2": &mockExemplarQuerier{res: getFixtureExemplarResult2()},
			}},
			matcher: [][]*labels.Matcher{{
				labels.MustNewMatcher(labels.MatchEqual, "__tenant_id__", "user-1"),
			}},
			orgId: "user-1|user-2",
			expectedResult: []exemplar.QueryResult{
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-1"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "123"),
							Value:  123,
							Ts:     1734942337900,
						},
					},
				},
			},
			expectedMetrics: expectedTwoTenantsExemplarMetrics,
		},
		{
			name: "when the '__tenant_id__' label exist, should be converted to the 'original___tenant_id__'",
			upstream: mockExemplarQueryable{exemplarQueriers: map[string]storage.ExemplarQuerier{
				"user-1": &mockExemplarQuerier{res: []exemplar.QueryResult{
					{
						SeriesLabels: labels.FromStrings("__name__", "exemplar_series", defaultTenantLabel, "tenant"),
						Exemplars: []exemplar.Exemplar{
							{
								Labels: labels.FromStrings("traceID", "123"),
								Value:  123,
								Ts:     1734942337900,
							},
						},
					},
				}},
				"user-2": &mockExemplarQuerier{res: getFixtureExemplarResult2()},
			}},
			matcher: [][]*labels.Matcher{{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "exemplar_series"),
			}},
			orgId: "user-1|user-2",
			expectedResult: []exemplar.QueryResult{
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-1", "original___tenant_id__", "tenant"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "123"),
							Value:  123,
							Ts:     1734942337900,
						},
					},
				},
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-2"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "456"),
							Value:  456,
							Ts:     1734942338000,
						},
					},
				},
			},
			expectedMetrics: expectedTwoTenantsExemplarMetrics,
		},
		{
			name: "get error from one querier, should get error",
			upstream: mockExemplarQueryable{exemplarQueriers: map[string]storage.ExemplarQuerier{
				"user-1": &mockExemplarQuerier{res: getFixtureExemplarResult1()},
				"user-2": &mockExemplarQuerier{err: errors.New("some error")},
			}},
			matcher: [][]*labels.Matcher{{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "exemplar_series"),
			}},
			orgId: "user-1|user-2",
			expectedResult: []exemplar.QueryResult{
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-1"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "123"),
							Value:  123,
							Ts:     1734942337900,
						},
					},
				},
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-2"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "456"),
							Value:  456,
							Ts:     1734942338000,
						},
					},
				},
			},
			expectedErr: errors.New("some error"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			exemplarQueryable := NewExemplarQueryable(&test.upstream, defaultMaxConcurrency, true, reg)
			ctx := user.InjectOrgID(context.Background(), test.orgId)
			q, err := exemplarQueryable.ExemplarQuerier(ctx)
			require.NoError(t, err)

			result, err := q.Select(mint, maxt, test.matcher...)
			if test.expectedErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(test.expectedMetrics), "cortex_querier_federated_tenants_per_exemplar_query"))
				require.Equal(t, test.expectedResult, result)
			}
		})
	}
}

func Test_MergeExemplarQuerier_Select_WhenUseRegexResolver(t *testing.T) {
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
	test.Poll(t, time.Second*10, true, func() any {
		return testutil.ToFloat64(regexResolver.lastUpdateUserRun) > 0 && testutil.ToFloat64(regexResolver.discoveredUsers) == 2
	})

	tests := []struct {
		name            string
		upstream        mockExemplarQueryable
		matcher         [][]*labels.Matcher
		orgId           string
		expectedResult  []exemplar.QueryResult
		expectedErr     error
		expectedMetrics string
	}{
		{
			name: "result labels should contains __tenant_id__ even if one tenant is queried",
			upstream: mockExemplarQueryable{exemplarQueriers: map[string]storage.ExemplarQuerier{
				"user-1": &mockExemplarQuerier{res: getFixtureExemplarResult1()},
				"user-2": &mockExemplarQuerier{res: getFixtureExemplarResult2()},
			}},
			matcher: [][]*labels.Matcher{{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "exemplar_series"),
			}},
			orgId: ".+-1",
			expectedResult: []exemplar.QueryResult{
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-1"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "123"),
							Value:  123,
							Ts:     1734942337900,
						},
					},
				},
			},
			expectedMetrics: expectedSingleTenantsExemplarMetrics,
		},
		{
			name: "two tenants results should be aggregated",
			upstream: mockExemplarQueryable{exemplarQueriers: map[string]storage.ExemplarQuerier{
				"user-1": &mockExemplarQuerier{res: getFixtureExemplarResult1()},
				"user-2": &mockExemplarQuerier{res: getFixtureExemplarResult2()},
			}},
			matcher: [][]*labels.Matcher{{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "exemplar_series"),
			}},
			orgId: "user-.+",
			expectedResult: []exemplar.QueryResult{
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-1"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "123"),
							Value:  123,
							Ts:     1734942337900,
						},
					},
				},
				{
					SeriesLabels: labels.FromStrings("__name__", "exemplar_series", "__tenant_id__", "user-2"),
					Exemplars: []exemplar.Exemplar{
						{
							Labels: labels.FromStrings("traceID", "456"),
							Value:  456,
							Ts:     1734942338000,
						},
					},
				},
			},
			expectedMetrics: expectedTwoTenantsExemplarMetrics,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			exemplarQueryable := NewExemplarQueryable(&test.upstream, defaultMaxConcurrency, false, reg)
			ctx := user.InjectOrgID(context.Background(), test.orgId)
			q, err := exemplarQueryable.ExemplarQuerier(ctx)
			require.NoError(t, err)

			result, err := q.Select(mint, maxt, test.matcher...)
			if test.expectedErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(test.expectedMetrics), "cortex_querier_federated_tenants_per_exemplar_query"))
				require.Equal(t, test.expectedResult, result)
			}
		})
	}
}

func Test_filterAllTenantsAndMatchers(t *testing.T) {
	idLabelName := defaultTenantLabel

	tests := []struct {
		name                           string
		tenantIds                      []string
		allMatchers                    [][]*labels.Matcher
		expectedLenAllMatchedTenantIds int
		expectedUnrelatedMatchersCnt   int
	}{
		{
			name:      "Should match all tenants",
			tenantIds: []string{"user-1", "user-2"},
			allMatchers: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				},
			},
			expectedLenAllMatchedTenantIds: 2,
			expectedUnrelatedMatchersCnt:   1,
		},
		{
			name:      "Should match target tenant with the `idLabelName` matcher",
			tenantIds: []string{"user-1", "user-2"},
			allMatchers: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, defaultTenantLabel, "user-1"),
				},
			},
			expectedLenAllMatchedTenantIds: 1,
			expectedUnrelatedMatchersCnt:   0,
		},
		{
			name:      "Should match all tenants with the retained label name matcher",
			tenantIds: []string{"user-1", "user-2"},
			allMatchers: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, retainExistingPrefix+defaultTenantLabel, "user-1"),
				},
			},
			expectedLenAllMatchedTenantIds: 2,
			expectedUnrelatedMatchersCnt:   1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allMatchedTenantIds, allUnrelatedMatchers := filterAllTenantsAndMatchers(idLabelName, test.tenantIds, test.allMatchers)
			matcherCnt := 0
			for _, unrelatedMatchers := range allUnrelatedMatchers {
				for _, matcher := range unrelatedMatchers {
					if matcher.Name != "" {
						matcherCnt++
					}
				}
			}
			require.Equal(t, test.expectedLenAllMatchedTenantIds, len(allMatchedTenantIds))
			require.Equal(t, test.expectedUnrelatedMatchersCnt, matcherCnt)
		})
	}
}
