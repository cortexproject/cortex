package ruler

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore/bucketclient"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func defaultRulerConfig(t testing.TB) Config {
	t.Helper()

	// Create a new temporary directory for the rules, so that
	// each test will run in isolation.
	rulesDir := t.TempDir()

	codec := ring.GetCodec()
	consul, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.RulePath = rulesDir
	cfg.Ring.KVStore.Mock = consul
	cfg.Ring.NumTokens = 1
	cfg.Ring.ListenPort = 0
	cfg.Ring.InstanceAddr = "localhost"
	cfg.Ring.InstanceID = "localhost"
	cfg.Ring.FinalSleep = 0
	cfg.Ring.ReplicationFactor = 1
	cfg.EnableQueryStats = false

	return cfg
}

type ruleLimits struct {
	evalDelay            time.Duration
	tenantShard          int
	maxRulesPerRuleGroup int
	maxRuleGroups        int
	disabledRuleGroups   validation.DisabledRuleGroups
	maxQueryLength       time.Duration
}

func (r ruleLimits) EvaluationDelay(_ string) time.Duration {
	return r.evalDelay
}

func (r ruleLimits) RulerTenantShardSize(_ string) int {
	return r.tenantShard
}

func (r ruleLimits) RulerMaxRuleGroupsPerTenant(_ string) int {
	return r.maxRuleGroups
}

func (r ruleLimits) RulerMaxRulesPerRuleGroup(_ string) int {
	return r.maxRulesPerRuleGroup
}

func (r ruleLimits) DisabledRuleGroups(userID string) validation.DisabledRuleGroups {
	return r.disabledRuleGroups
}

func (r ruleLimits) MaxQueryLength(_ string) time.Duration { return r.maxQueryLength }

func newEmptyQueryable() storage.Queryable {
	return storage.QueryableFunc(func(mint, maxt int64) (storage.Querier, error) {
		return emptyQuerier{}, nil
	})
}

type emptyQuerier struct {
}

func (e emptyQuerier) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (e emptyQuerier) LabelNames(ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (e emptyQuerier) Close() error {
	return nil
}

func (e emptyQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}

func testQueryableFunc(querierTestConfig *querier.TestConfig, reg prometheus.Registerer, logger log.Logger) storage.QueryableFunc {
	if querierTestConfig != nil {
		// disable active query tracking for test
		querierTestConfig.Cfg.ActiveQueryTrackerDir = ""

		overrides, _ := validation.NewOverrides(querier.DefaultLimitsConfig(), nil)
		q, _, _ := querier.New(querierTestConfig.Cfg, overrides, querierTestConfig.Distributor, querierTestConfig.Stores, reg, logger)
		return func(mint, maxt int64) (storage.Querier, error) {
			return q.Querier(mint, maxt)
		}
	}

	return func(mint, maxt int64) (storage.Querier, error) {
		return storage.NoopQuerier(), nil
	}
}

func testSetup(t *testing.T, querierTestConfig *querier.TestConfig) (*promql.Engine, storage.QueryableFunc, Pusher, log.Logger, RulesLimits, prometheus.Registerer) {
	tracker := promql.NewActiveQueryTracker(t.TempDir(), 20, log.NewNopLogger())

	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:         1e6,
		ActiveQueryTracker: tracker,
		Timeout:            2 * time.Minute,
	})

	// Mock the pusher
	pusher := newPusherMock()
	pusher.MockPush(&cortexpb.WriteResponse{}, nil)

	l := log.NewLogfmtLogger(os.Stdout)
	l = level.NewFilter(l, level.AllowInfo())

	reg := prometheus.NewRegistry()
	queryable := testQueryableFunc(querierTestConfig, reg, l)

	return engine, queryable, pusher, l, ruleLimits{evalDelay: 0, maxRuleGroups: 20, maxRulesPerRuleGroup: 15}, reg
}

func newManager(t *testing.T, cfg Config) *DefaultMultiTenantManager {
	engine, queryable, pusher, logger, overrides, reg := testSetup(t, nil)
	metrics := NewRuleEvalMetrics(cfg, nil)
	managerFactory := DefaultTenantManagerFactory(cfg, pusher, queryable, engine, overrides, metrics, nil)
	manager, err := NewDefaultMultiTenantManager(cfg, managerFactory, metrics, reg, logger)
	require.NoError(t, err)

	return manager
}

type mockRulerClientsPool struct {
	ClientsPool
	cfg           Config
	rulerAddrMap  map[string]*Ruler
	numberOfCalls atomic.Int32
}

type mockRulerClient struct {
	ruler         *Ruler
	numberOfCalls *atomic.Int32
}

func (c *mockRulerClient) Rules(ctx context.Context, in *RulesRequest, _ ...grpc.CallOption) (*RulesResponse, error) {
	c.numberOfCalls.Inc()
	return c.ruler.Rules(ctx, in)
}

func (p *mockRulerClientsPool) GetClientFor(addr string) (RulerClient, error) {
	for _, r := range p.rulerAddrMap {
		if r.lifecycler.GetInstanceAddr() == addr {
			return &mockRulerClient{
				ruler:         r,
				numberOfCalls: &p.numberOfCalls,
			}, nil
		}
	}

	return nil, fmt.Errorf("unable to find ruler for add %s", addr)
}

func newMockClientsPool(cfg Config, logger log.Logger, reg prometheus.Registerer, rulerAddrMap map[string]*Ruler) *mockRulerClientsPool {
	return &mockRulerClientsPool{
		ClientsPool:  newRulerClientPool(cfg.ClientTLSConfig, logger, reg),
		cfg:          cfg,
		rulerAddrMap: rulerAddrMap,
	}
}

func buildRuler(t *testing.T, rulerConfig Config, querierTestConfig *querier.TestConfig, store rulestore.RuleStore, rulerAddrMap map[string]*Ruler) (*Ruler, *DefaultMultiTenantManager) {
	engine, queryable, pusher, logger, overrides, reg := testSetup(t, querierTestConfig)
	metrics := NewRuleEvalMetrics(rulerConfig, reg)
	managerFactory := DefaultTenantManagerFactory(rulerConfig, pusher, queryable, engine, overrides, metrics, reg)
	manager, err := NewDefaultMultiTenantManager(rulerConfig, managerFactory, metrics, reg, log.NewNopLogger())
	require.NoError(t, err)

	ruler, err := newRuler(
		rulerConfig,
		manager,
		reg,
		logger,
		store,
		overrides,
		newMockClientsPool(rulerConfig, logger, reg, rulerAddrMap),
	)
	require.NoError(t, err)
	return ruler, manager
}

func newTestRuler(t *testing.T, rulerConfig Config, store rulestore.RuleStore, querierTestConfig *querier.TestConfig) *Ruler {
	ruler, _ := buildRuler(t, rulerConfig, querierTestConfig, store, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ruler))

	// Ensure all rules are loaded before usage
	ruler.syncRules(context.Background(), rulerSyncReasonInitial)

	return ruler
}

var _ MultiTenantManager = &DefaultMultiTenantManager{}

func TestNotifierSendsUserIDHeader(t *testing.T) {
	var wg sync.WaitGroup

	// We do expect 1 API call for the user create with the getOrCreateNotifier()
	wg.Add(1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, _, err := tenant.ExtractTenantIDFromHTTPRequest(r)
		assert.NoError(t, err)
		assert.Equal(t, userID, "1")
		wg.Done()
	}))
	defer ts.Close()

	cfg := defaultRulerConfig(t)

	cfg.AlertmanagerURL = ts.URL
	cfg.AlertmanagerDiscovery = false

	manager := newManager(t, cfg)
	defer manager.Stop()

	n, err := manager.getOrCreateNotifier("1", manager.registry)
	require.NoError(t, err)

	// Loop until notifier discovery syncs up
	for len(n.Alertmanagers()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	n.Send(&notifier.Alert{
		Labels: labels.Labels{labels.Label{Name: "alertname", Value: "testalert"}},
	})

	wg.Wait()

	// Ensure we have metrics in the notifier.
	assert.NoError(t, prom_testutil.GatherAndCompare(manager.registry.(*prometheus.Registry), strings.NewReader(`
		# HELP prometheus_notifications_dropped_total Total number of alerts dropped due to errors when sending to Alertmanager.
		# TYPE prometheus_notifications_dropped_total counter
		prometheus_notifications_dropped_total 0
	`), "prometheus_notifications_dropped_total"))
}

func TestRuler_Rules(t *testing.T) {
	store := newMockRuleStore(mockRules, nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	// test user1
	ctx := user.InjectOrgID(context.Background(), "user1")
	rls, err := r.Rules(ctx, &RulesRequest{})
	require.NoError(t, err)
	require.Len(t, rls.Groups, 1)
	rg := rls.Groups[0]
	expectedRg := mockRules["user1"][0]
	compareRuleGroupDescToStateDesc(t, expectedRg, rg)

	// test user2
	ctx = user.InjectOrgID(context.Background(), "user2")
	rls, err = r.Rules(ctx, &RulesRequest{})
	require.NoError(t, err)
	require.Len(t, rls.Groups, 1)
	rg = rls.Groups[0]
	expectedRg = mockRules["user2"][0]
	compareRuleGroupDescToStateDesc(t, expectedRg, rg)
}

func compareRuleGroupDescToStateDesc(t *testing.T, expected *rulespb.RuleGroupDesc, got *GroupStateDesc) {
	require.Equal(t, got.Group.Name, expected.Name)
	require.Equal(t, got.Group.Namespace, expected.Namespace)
	require.Len(t, expected.Rules, len(got.ActiveRules))
	for i := range got.ActiveRules {
		require.Equal(t, expected.Rules[i].Record, got.ActiveRules[i].Rule.Record)
		require.Equal(t, expected.Rules[i].Alert, got.ActiveRules[i].Rule.Alert)
	}
}

func TestGetRules(t *testing.T) {
	// ruler ID -> (user ID -> list of groups).
	type expectedRulesMap map[string]map[string]rulespb.RuleGroupList
	type rulesMap map[string][]*rulespb.RuleDesc

	type testCase struct {
		sharding                   bool
		shardingStrategy           string
		shuffleShardSize           int
		rulesRequest               RulesRequest
		expectedCount              map[string]int
		expectedClientCallCount    int
		rulerStateMap              map[string]ring.InstanceState
		rulerAZMap                 map[string]string
		expectedError              error
		replicationFactor          int
		enableZoneAwareReplication bool
	}

	ruleMap := rulesMap{
		"ruler1-user1-rule-group1": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "rulename", Value: "rtest_user1_group1_rule_1"},
				},
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "alertname", Value: "atest_user1_group1_rule_1"},
				},
			},
		},
		"ruler1-user1-rule-group2": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "rulename", Value: "rtest_user1_group2_rule_1"},
				},
			},
		},
		"ruler1-user2-rule-group1": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "rulename", Value: "rtest_user2_group1_rule_1"},
				},
			},
		},
		"ruler2-user1-rule-group3": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "rulename", Value: "rtest_user1_group3_rule_1"},
				},
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "alertname", Value: "atest_user1_group3_rule_1"},
				},
			},
		},
		"ruler2-user2-rule-group1": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "rulename", Value: "rtest_user2_group1_rule_1"},
				},
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "alertname", Value: "atest_user2_group1_rule_1"},
				},
			},
		},
		"ruler2-user2-rule-group2": []*rulespb.RuleDesc{
			{
				Record: "rtest_user2_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "rulename", Value: "rtest_user2_group2_rule_1"},
				},
			},
			{
				Alert: "atest_user2_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "alertname", Value: "atest_user2_group2_rule_1"},
				},
			},
		},
		"ruler2-user3-rule-group1": []*rulespb.RuleDesc{
			{
				Alert: "atest_user3_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "alertname", Value: "atest_user3_group1_rule_1"},
				},
			},
		},
		"ruler3-user2-rule-group1": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "rulename", Value: "rtest_user2_group1_rule_1"},
				},
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "alertname", Value: "atest_user2_group1_rule_1"},
				},
			},
		},
		"ruler3-user2-rule-group2": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "rulename", Value: "rtest_user2_group2_rule_1"},
				},
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "alertname", Value: "atest_user2_group2_rule_1"},
				},
			},
		},
		"ruler3-user3-rule-group1": []*rulespb.RuleDesc{
			{
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Record: "rtest_user1_1",
				Labels: []cortexpb.LabelAdapter{
					{Name: "templatedlabel", Value: "{{ $externalURL }}"},
				},
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Labels: []cortexpb.LabelAdapter{
					{Name: "alertname", Value: "atest_user3_group1_rule_1"},
				},
			},
		},
	}

	rulerStateMapAllActive := map[string]ring.InstanceState{
		"ruler1": ring.ACTIVE,
		"ruler2": ring.ACTIVE,
		"ruler3": ring.ACTIVE,
	}

	rulerStateMapOneLeaving := map[string]ring.InstanceState{
		"ruler1": ring.ACTIVE,
		"ruler2": ring.LEAVING,
		"ruler3": ring.ACTIVE,
	}

	rulerStateMapOnePending := map[string]ring.InstanceState{
		"ruler1": ring.ACTIVE,
		"ruler2": ring.PENDING,
		"ruler3": ring.ACTIVE,
	}

	rulerStateMapTwoPending := map[string]ring.InstanceState{
		"ruler1": ring.PENDING,
		"ruler2": ring.PENDING,
		"ruler3": ring.ACTIVE,
	}

	rulerAZEvenSpread := map[string]string{
		"ruler1": "a",
		"ruler2": "b",
		"ruler3": "c",
	}
	rulerAZSingleZone := map[string]string{
		"ruler1": "a",
		"ruler2": "a",
		"ruler3": "a",
	}

	expectedRules := expectedRulesMap{
		"ruler1": map[string]rulespb.RuleGroupList{
			"user1": {
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "first", Interval: 10 * time.Second, Rules: ruleMap["ruler1-user1-rule-group1"]},
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "second", Interval: 10 * time.Second, Rules: ruleMap["ruler1-user1-rule-group2"]},
			},
			"user2": {
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "third", Interval: 10 * time.Second, Rules: ruleMap["ruler1-user2-rule-group1"]},
			},
		},
		"ruler2": map[string]rulespb.RuleGroupList{
			"user1": {
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "third", Interval: 10 * time.Second, Rules: ruleMap["ruler2-user1-rule-group3"]},
			},
			"user2": {
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "first", Interval: 10 * time.Second, Rules: ruleMap["ruler2-user2-rule-group1"]},
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "second", Interval: 10 * time.Second, Rules: ruleMap["ruler2-user2-rule-group2"]},
			},
			"user3": {
				&rulespb.RuleGroupDesc{User: "user3", Namespace: "latency-test", Name: "first", Interval: 10 * time.Second, Rules: ruleMap["ruler2-user3-rule-group1"]},
			},
		},
		"ruler3": map[string]rulespb.RuleGroupList{
			"user3": {
				&rulespb.RuleGroupDesc{User: "user3", Namespace: "namespace", Name: "third", Interval: 10 * time.Second, Rules: ruleMap["ruler3-user3-rule-group1"]},
			},
			"user2": {
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "forth", Interval: 10 * time.Second, Rules: ruleMap["ruler3-user2-rule-group1"]},
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "fifty", Interval: 10 * time.Second, Rules: ruleMap["ruler3-user2-rule-group2"]},
			},
		},
	}

	testCases := map[string]testCase{
		"No Sharding with Rule Type Filter": {
			sharding: false,
			rulesRequest: RulesRequest{
				Type: alertingRuleFilter,
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 2,
				"user2": 4,
				"user3": 2,
			},
			expectedClientCallCount: len(expectedRules),
		},
		"No Sharding with Alert state filter for firing alerts": {
			sharding: false,
			rulesRequest: RulesRequest{
				State: firingStateFilter,
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 0,
				"user2": 0,
				"user3": 0,
			},
		},
		"No Sharding with Alert state filter for inactive alerts": {
			sharding: false,
			rulesRequest: RulesRequest{
				State: inactiveStateFilter,
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 2,
				"user2": 4,
				"user3": 2,
			},
		},
		"No Sharding with health filter for OK alerts": {
			sharding: false,
			rulesRequest: RulesRequest{
				Health: okHealthFilter,
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 0,
				"user2": 0,
				"user3": 0,
			},
		},
		"No Sharding with health filter for unknown alerts": {
			sharding: false,
			rulesRequest: RulesRequest{
				Health: unknownHealthFilter,
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 5,
				"user2": 9,
				"user3": 3,
			},
		},
		"No Sharding with Rule label matcher filter - match 1 rule": {
			sharding: false,
			rulesRequest: RulesRequest{
				Matchers: []string{`{alertname="atest_user1_group1_rule_1"}`},
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 1,
				"user2": 0,
				"user3": 0,
			},
		},
		"No Sharding with Rule label matcher filter - label match all alerting rule": {
			sharding: false,
			rulesRequest: RulesRequest{
				Matchers: []string{`{alertname=~"atest_.*"}`},
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 2,
				"user2": 4,
				"user3": 2,
			},
		},
		"Default Sharding with No Filter": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			rulerStateMap:    rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 5,
				"user2": 9,
				"user3": 3,
			},
			expectedClientCallCount: len(expectedRules),
		},
		"Default Sharding with No Filter but with API Rules backup enabled": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			rulerStateMap:    rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 5,
				"user2": 9,
				"user3": 3,
			},
			replicationFactor:       3,
			expectedClientCallCount: len(expectedRules),
		},
		"Shuffle Sharding and ShardSize = 2 with Rule Type Filter": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulerStateMap:    rulerStateMapAllActive,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 3,
				"user2": 5,
				"user3": 1,
			},
			expectedClientCallCount: 2,
		},
		"Shuffle Sharding and ShardSize = 2 and Rule Group Name Filter": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulesRequest: RulesRequest{
				RuleGroupNames: []string{"third"},
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 2,
				"user2": 1,
				"user3": 2,
			},
			expectedClientCallCount: 2,
		},
		"Shuffle Sharding and ShardSize = 2 and Rule Group Name and Rule Type Filter": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulerStateMap:    rulerStateMapAllActive,
			rulesRequest: RulesRequest{
				RuleGroupNames: []string{"second", "third"},
				Type:           recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 2,
				"user2": 2,
				"user3": 1,
			},
			expectedClientCallCount: 2,
		},
		"Shuffle Sharding and ShardSize = 2 with Rule Type and Namespace Filters": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulerStateMap:    rulerStateMapAllActive,
			rulesRequest: RulesRequest{
				Type:  alertingRuleFilter,
				Files: []string{"latency-test"},
			},
			expectedCount: map[string]int{
				"user1": 0,
				"user2": 0,
				"user3": 1,
			},
			expectedClientCallCount: 2,
		},
		"Shuffle Sharding and ShardSize = 2 with Rule Type Filter and one ruler is in LEAVING state": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulerStateMap:    rulerStateMapOneLeaving,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 3,
				"user2": 5,
				"user3": 1,
			},
			expectedClientCallCount: 2,
		},
		"Shuffle Sharding and ShardSize = 2 with Rule Type Filter and one ruler is in Pending state": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulerStateMap:    rulerStateMapOnePending,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedError:           ring.ErrTooManyUnhealthyInstances,
			expectedClientCallCount: 0,
		},
		"Shuffle Sharding and ShardSize = 2 with Rule label Filter": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulesRequest: RulesRequest{
				Matchers: []string{`{alertname="atest_user1_group1_rule_1"}`},
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 1,
				"user2": 0,
				"user3": 0,
			},
			expectedClientCallCount: 2,
		},
		"Shuffle Sharding and ShardSize = 2 with Rule label Filter match 2 rules": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulesRequest: RulesRequest{
				Matchers: []string{`{alertname="atest_user1_group1_rule_1"}`, `{alertname="atest_user2_group1_rule_1"}`},
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 1,
				"user2": 2,
				"user3": 0,
			},
			expectedClientCallCount: 2,
		},
		"Shuffle Sharding and ShardSize = 2 with Rule label Filter match templating label": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulesRequest: RulesRequest{
				Matchers: []string{`{templatedlabel="{{ $externalURL }}"}`},
			},
			rulerStateMap: rulerStateMapAllActive,
			expectedCount: map[string]int{
				"user1": 0,
				"user2": 0,
				"user3": 1,
			},
			expectedClientCallCount: 2,
		},
		"Shuffle Sharding and ShardSize = 3 with API Rules backup enabled with labels filter": {
			sharding:          true,
			shuffleShardSize:  3,
			shardingStrategy:  util.ShardingStrategyShuffle,
			rulerStateMap:     rulerStateMapAllActive,
			replicationFactor: 3,
			rulesRequest: RulesRequest{
				Matchers: []string{`{alertname="atest_user1_group1_rule_1"}`, `{alertname="atest_user2_group1_rule_1"}`},
			},
			expectedCount: map[string]int{
				"user1": 1,
				"user2": 2,
				"user3": 0,
			},
			expectedClientCallCount: 3,
		},
		"Shuffle Sharding and ShardSize = 3 with API Rules backup enabled": {
			sharding:          true,
			shuffleShardSize:  3,
			shardingStrategy:  util.ShardingStrategyShuffle,
			rulerStateMap:     rulerStateMapAllActive,
			replicationFactor: 3,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 3,
				"user2": 5,
				"user3": 1,
			},
			expectedClientCallCount: 3,
		},
		"Shuffle Sharding and ShardSize = 3 with API Rules backup enabled and one ruler is in Pending state": {
			sharding:          true,
			shuffleShardSize:  3,
			shardingStrategy:  util.ShardingStrategyShuffle,
			rulerStateMap:     rulerStateMapOnePending,
			replicationFactor: 3,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 3,
				"user2": 5,
				"user3": 1,
			},
			expectedClientCallCount: 2, // one of the ruler is pending, so we don't expect that ruler to be called
		},
		"Shuffle Sharding and ShardSize = 3 with API Rules backup enabled and two ruler is in Pending state": {
			sharding:          true,
			shuffleShardSize:  3,
			shardingStrategy:  util.ShardingStrategyShuffle,
			rulerStateMap:     rulerStateMapTwoPending,
			replicationFactor: 3,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedError: ring.ErrTooManyUnhealthyInstances,
		},
		"Shuffle Sharding and ShardSize = 3 and AZ replication with API Rules backup enabled": {
			sharding:                   true,
			shuffleShardSize:           3,
			shardingStrategy:           util.ShardingStrategyShuffle,
			enableZoneAwareReplication: true,
			rulerStateMap:              rulerStateMapAllActive,
			rulerAZMap:                 rulerAZEvenSpread,
			replicationFactor:          3,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 3,
				"user2": 5,
				"user3": 1,
			},
			expectedClientCallCount: 3,
		},
		"Shuffle Sharding and ShardSize = 3 and AZ replication with API Rules backup enabled and one ruler in pending state": {
			sharding:                   true,
			shuffleShardSize:           3,
			shardingStrategy:           util.ShardingStrategyShuffle,
			enableZoneAwareReplication: true,
			rulerStateMap:              rulerStateMapOnePending,
			rulerAZMap:                 rulerAZEvenSpread,
			replicationFactor:          3,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 3,
				"user2": 5,
				"user3": 1,
			},
			expectedClientCallCount: 2, // one of the ruler is pending, so we don't expect that ruler to be called
		},
		"Shuffle Sharding and ShardSize = 3 and AZ replication with API Rules backup enabled and one ruler in pending state and rulers are in same az": {
			sharding:                   true,
			shuffleShardSize:           3,
			shardingStrategy:           util.ShardingStrategyShuffle,
			enableZoneAwareReplication: true,
			rulerStateMap:              rulerStateMapOnePending,
			rulerAZMap:                 rulerAZSingleZone,
			replicationFactor:          3,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 3,
				"user2": 5,
				"user3": 1,
			},
			expectedClientCallCount: 2, // one of the ruler is pending, so we don't expect that ruler to be called
		},
		"Shuffle Sharding and ShardSize = 3 and AZ replication with API Rules backup enabled and two ruler in pending state": {
			sharding:                   true,
			shuffleShardSize:           3,
			shardingStrategy:           util.ShardingStrategyShuffle,
			enableZoneAwareReplication: true,
			rulerStateMap:              rulerStateMapTwoPending,
			rulerAZMap:                 rulerAZEvenSpread,
			replicationFactor:          3,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedError: ring.ErrTooManyUnhealthyInstances,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			kvStore, cleanUp := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, cleanUp.Close()) })
			allRulesByUser := map[string]rulespb.RuleGroupList{}
			allRulesByRuler := map[string]rulespb.RuleGroupList{}
			allTokensByRuler := map[string][]uint32{}
			rulerAddrMap := map[string]*Ruler{}

			createRuler := func(id string) *Ruler {
				store := newMockRuleStore(allRulesByUser, nil)
				cfg := defaultRulerConfig(t)

				cfg.ShardingStrategy = tc.shardingStrategy
				cfg.EnableSharding = tc.sharding

				cfg.Ring = RingConfig{
					InstanceID:   id,
					InstanceAddr: id,
					KVStore: kv.Config{
						Mock: kvStore,
					},
					ReplicationFactor: 1,
				}
				if tc.replicationFactor > 0 {
					cfg.Ring.ReplicationFactor = tc.replicationFactor
					cfg.Ring.ZoneAwarenessEnabled = tc.enableZoneAwareReplication
				}
				if tc.enableZoneAwareReplication {
					cfg.Ring.InstanceZone = tc.rulerAZMap[id]
				}

				r, _ := buildRuler(t, cfg, nil, store, rulerAddrMap)
				r.limits = ruleLimits{evalDelay: 0, tenantShard: tc.shuffleShardSize}
				rulerAddrMap[id] = r
				if r.ring != nil {
					require.NoError(t, services.StartAndAwaitRunning(context.Background(), r.ring))
					t.Cleanup(r.ring.StopAsync)
				}
				return r
			}

			for rID, r := range expectedRules {
				createRuler(rID)
				for user, rules := range r {
					allRulesByUser[user] = append(allRulesByUser[user], rules...)
					allRulesByRuler[rID] = append(allRulesByRuler[rID], rules...)
					allTokensByRuler[rID] = generateTokenForGroups(rules, 1)
				}
			}

			if tc.sharding {
				err := kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
					d, _ := in.(*ring.Desc)
					if d == nil {
						d = ring.NewDesc()
					}
					for rID, tokens := range allTokensByRuler {
						d.AddIngester(rID, rulerAddrMap[rID].lifecycler.GetInstanceAddr(), rulerAddrMap[rID].lifecycler.GetInstanceZone(), tokens, ring.ACTIVE, time.Now())
					}
					return d, true, nil
				})
				require.NoError(t, err)
				// Wait a bit to make sure ruler's ring is updated.
				time.Sleep(100 * time.Millisecond)
			}

			forEachRuler := func(f func(rID string, r *Ruler)) {
				for rID, r := range rulerAddrMap {
					f(rID, r)
				}
			}

			// Sync Rules
			forEachRuler(func(_ string, r *Ruler) {
				r.syncRules(context.Background(), rulerSyncReasonInitial)
			})

			if tc.sharding {
				// update the State of the rulers in the ring based on tc.rulerStateMap
				err := kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
					d, _ := in.(*ring.Desc)
					if d == nil {
						d = ring.NewDesc()
					}
					for rID, tokens := range allTokensByRuler {
						d.AddIngester(rID, rulerAddrMap[rID].lifecycler.GetInstanceAddr(), rulerAddrMap[rID].lifecycler.GetInstanceZone(), tokens, tc.rulerStateMap[rID], time.Now())
					}
					return d, true, nil
				})
				require.NoError(t, err)
				// Wait a bit to make sure ruler's ring is updated.
				time.Sleep(100 * time.Millisecond)
			}

			for u := range allRulesByUser {
				ctx := user.InjectOrgID(context.Background(), u)
				forEachRuler(func(_ string, r *Ruler) {
					ruleStateDescriptions, err := r.GetRules(ctx, tc.rulesRequest)
					if tc.expectedError != nil {
						require.Error(t, tc.expectedError)
						return
					} else {
						require.NoError(t, err)
					}
					rct := 0
					for _, ruleStateDesc := range ruleStateDescriptions {
						rct += len(ruleStateDesc.ActiveRules)
					}
					require.Equal(t, tc.expectedCount[u], rct)
					if tc.sharding {
						mockPoolClient := r.clientsPool.(*mockRulerClientsPool)

						if tc.shardingStrategy == util.ShardingStrategyShuffle {
							require.Equal(t, int32(tc.expectedClientCallCount), mockPoolClient.numberOfCalls.Load())
						} else {
							require.Equal(t, int32(tc.expectedClientCallCount), mockPoolClient.numberOfCalls.Load())
						}
						mockPoolClient.numberOfCalls.Store(0)
					}
				})
			}

			totalLoadedRules := 0
			totalConfiguredRules := 0
			ruleBackupCount := make(map[string]int)

			forEachRuler(func(rID string, r *Ruler) {
				localRules, localBackupRules, err := r.listRules(context.Background())
				require.NoError(t, err)
				for _, rules := range localRules {
					totalLoadedRules += len(rules)
				}
				for user, rules := range localBackupRules {
					for _, rule := range rules {
						key := user + rule.Namespace + rule.Name
						c := ruleBackupCount[key]
						ruleBackupCount[key] = c + 1
					}
				}
				totalConfiguredRules += len(allRulesByRuler[rID])
			})

			if tc.sharding {
				require.Equal(t, totalConfiguredRules, totalLoadedRules)
			} else {
				// Not sharding means that all rules will be loaded on all rulers
				numberOfRulers := len(rulerAddrMap)
				require.Equal(t, totalConfiguredRules*numberOfRulers, totalLoadedRules)
			}
			if tc.replicationFactor > 1 && tc.sharding && tc.expectedError == nil {
				// all rules should be backed up
				require.Equal(t, totalConfiguredRules, len(ruleBackupCount))
				var hasUnhealthyRuler bool
				for _, state := range tc.rulerStateMap {
					if state != ring.ACTIVE && state != ring.LEAVING {
						hasUnhealthyRuler = true
						break
					}
				}
				for _, v := range ruleBackupCount {
					if !hasUnhealthyRuler {
						// with replication factor set to 3, each rule is backed up by 2 rulers
						require.Equal(t, 2, v)
					} else {
						require.GreaterOrEqual(t, v, 1)
					}
				}
			} else {
				// If rules backup is disabled, rulers should not back up any rules
				require.Equal(t, 0, len(ruleBackupCount))
			}
		})
	}
}

func TestGetRulesFromBackup(t *testing.T) {
	// ruler ID -> (user ID -> list of groups).
	type expectedRulesMap map[string]map[string]rulespb.RuleGroupList

	rule := []*rulespb.RuleDesc{
		{
			Record: "rtest_user1_1",
			Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
		},
		{
			Alert: "atest_user1_1",
			Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
		},
		{
			Record: "rtest_user1_2",
			Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
			Labels: []cortexpb.LabelAdapter{
				{Name: "key", Value: "val"},
			},
		},
		{
			Alert: "atest_user1_2",
			Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
			Labels: []cortexpb.LabelAdapter{
				{Name: "key", Value: "val"},
			},
			Annotations: []cortexpb.LabelAdapter{
				{Name: "aKey", Value: "aVal"},
			},
			For:           10 * time.Second,
			KeepFiringFor: 20 * time.Second,
		},
	}

	tenantId := "user1"

	rulerStateMapOnePending := map[string]ring.InstanceState{
		"ruler1": ring.ACTIVE,
		"ruler2": ring.PENDING,
		"ruler3": ring.ACTIVE,
	}

	rulerAZEvenSpread := map[string]string{
		"ruler1": "a",
		"ruler2": "b",
		"ruler3": "c",
	}

	expectedRules := expectedRulesMap{
		"ruler1": map[string]rulespb.RuleGroupList{
			tenantId: {
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "l1", Interval: 10 * time.Minute, Limit: 10, Rules: rule},
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "l2", Interval: 0, Rules: rule},
			},
		},
		"ruler2": map[string]rulespb.RuleGroupList{
			tenantId: {
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "b1", Interval: 10 * time.Minute, Limit: 10, Rules: rule},
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "b2", Interval: 0, Rules: rule},
			},
		},
		"ruler3": map[string]rulespb.RuleGroupList{
			tenantId: {
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace2", Name: "b3", Interval: 0, Rules: rule},
			},
		},
	}

	kvStore, cleanUp := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, cleanUp.Close()) })
	allRulesByUser := map[string]rulespb.RuleGroupList{}
	allTokensByRuler := map[string][]uint32{}
	rulerAddrMap := map[string]*Ruler{}

	createRuler := func(id string) *Ruler {
		store := newMockRuleStore(allRulesByUser, nil)
		cfg := defaultRulerConfig(t)

		cfg.ShardingStrategy = util.ShardingStrategyShuffle
		cfg.EnableSharding = true
		cfg.EvaluationInterval = 5 * time.Minute

		cfg.Ring = RingConfig{
			InstanceID:   id,
			InstanceAddr: id,
			KVStore: kv.Config{
				Mock: kvStore,
			},
			ReplicationFactor:    3,
			ZoneAwarenessEnabled: true,
			InstanceZone:         rulerAZEvenSpread[id],
		}

		r, _ := buildRuler(t, cfg, nil, store, rulerAddrMap)
		r.limits = ruleLimits{evalDelay: 0, tenantShard: 3}
		rulerAddrMap[id] = r
		if r.ring != nil {
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), r.ring))
			t.Cleanup(r.ring.StopAsync)
		}
		return r
	}

	for rID, r := range expectedRules {
		createRuler(rID)
		for u, rules := range r {
			allRulesByUser[u] = append(allRulesByUser[u], rules...)
			allTokensByRuler[rID] = generateTokenForGroups(rules, 1)
		}
	}

	err := kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		d, _ := in.(*ring.Desc)
		if d == nil {
			d = ring.NewDesc()
		}
		for rID, tokens := range allTokensByRuler {
			d.AddIngester(rID, rulerAddrMap[rID].lifecycler.GetInstanceAddr(), rulerAddrMap[rID].lifecycler.GetInstanceZone(), tokens, ring.ACTIVE, time.Now())
		}
		return d, true, nil
	})
	require.NoError(t, err)
	// Wait a bit to make sure ruler's ring is updated.
	time.Sleep(100 * time.Millisecond)

	forEachRuler := func(f func(rID string, r *Ruler)) {
		for rID, r := range rulerAddrMap {
			f(rID, r)
		}
	}

	// Sync Rules
	forEachRuler(func(_ string, r *Ruler) {
		r.syncRules(context.Background(), rulerSyncReasonInitial)
	})

	// update the State of the rulers in the ring based on tc.rulerStateMap
	err = kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		d, _ := in.(*ring.Desc)
		if d == nil {
			d = ring.NewDesc()
		}
		for rID, tokens := range allTokensByRuler {
			d.AddIngester(rID, rulerAddrMap[rID].lifecycler.GetInstanceAddr(), rulerAddrMap[rID].lifecycler.GetInstanceZone(), tokens, rulerStateMapOnePending[rID], time.Now())
		}
		return d, true, nil
	})
	require.NoError(t, err)
	// Wait a bit to make sure ruler's ring is updated.
	time.Sleep(100 * time.Millisecond)

	requireGroupStateEqual := func(a *GroupStateDesc, b *GroupStateDesc) {
		require.Equal(t, a.Group.Interval, b.Group.Interval)
		require.Equal(t, a.Group.User, b.Group.User)
		require.Equal(t, a.Group.Limit, b.Group.Limit)
		require.Equal(t, a.EvaluationTimestamp, b.EvaluationTimestamp)
		require.Equal(t, a.EvaluationDuration, b.EvaluationDuration)
		require.Equal(t, len(a.ActiveRules), len(b.ActiveRules))
		for i, aRule := range a.ActiveRules {
			bRule := b.ActiveRules[i]
			require.Equal(t, aRule.EvaluationTimestamp, bRule.EvaluationTimestamp)
			require.Equal(t, aRule.EvaluationDuration, bRule.EvaluationDuration)
			require.Equal(t, aRule.Health, bRule.Health)
			require.Equal(t, aRule.LastError, bRule.LastError)
			require.Equal(t, aRule.Rule.Expr, bRule.Rule.Expr)
			require.Equal(t, len(aRule.Rule.Labels), len(bRule.Rule.Labels))
			require.Equal(t, fmt.Sprintf("%+v", aRule.Rule.Labels), fmt.Sprintf("%+v", aRule.Rule.Labels))
			if aRule.Rule.Alert != "" {
				require.Equal(t, fmt.Sprintf("%+v", aRule.Rule.Annotations), fmt.Sprintf("%+v", bRule.Rule.Annotations))
				require.Equal(t, aRule.Rule.Alert, bRule.Rule.Alert)
				require.Equal(t, aRule.Rule.For, bRule.Rule.For)
				require.Equal(t, aRule.Rule.KeepFiringFor, bRule.Rule.KeepFiringFor)
				require.Equal(t, aRule.State, bRule.State)
				require.Equal(t, aRule.Alerts, bRule.Alerts)
			} else {
				require.Equal(t, aRule.Rule.Record, bRule.Rule.Record)
			}
		}
	}
	ctx := user.InjectOrgID(context.Background(), tenantId)
	ruleStateDescriptions, err := rulerAddrMap["ruler1"].GetRules(ctx, RulesRequest{})
	require.NoError(t, err)
	require.Equal(t, 5, len(ruleStateDescriptions))
	stateByKey := map[string]*GroupStateDesc{}
	for _, state := range ruleStateDescriptions {
		stateByKey[state.Group.Namespace+";"+state.Group.Name] = state
	}
	// Rule Group Name that starts will b are from the backup and those that start with l are evaluating, the details of
	// the group other than the Name should be equal to the group that starts with l as the config is the same. This test
	// confirms that the way we convert rulepb.RuleGroupList to GroupStateDesc is consistent to how we convert
	// promRules.Group to GroupStateDesc
	requireGroupStateEqual(stateByKey["namespace;l1"], stateByKey["namespace;b1"])
	requireGroupStateEqual(stateByKey["namespace;l2"], stateByKey["namespace;b2"])

	// Validate backup rules respect the filters
	ruleStateDescriptions, err = rulerAddrMap["ruler1"].GetRules(ctx, RulesRequest{
		RuleNames:      []string{"rtest_user1_1", "atest_user1_1"},
		Files:          []string{"namespace"},
		RuleGroupNames: []string{"b1"},
		Type:           recordingRuleFilter,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(ruleStateDescriptions))
	require.Equal(t, "b1", ruleStateDescriptions[0].Group.Name)
	require.Equal(t, 1, len(ruleStateDescriptions[0].ActiveRules))
	require.Equal(t, "rtest_user1_1", ruleStateDescriptions[0].ActiveRules[0].Rule.Record)
}

func TestSharding(t *testing.T) {
	const (
		user1 = "user1"
		user2 = "user2"
		user3 = "user3"
	)

	user1Group1 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "first"}
	user1Group2 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "second"}
	user2Group1 := &rulespb.RuleGroupDesc{User: user2, Namespace: "namespace", Name: "first"}
	user3Group1 := &rulespb.RuleGroupDesc{User: user3, Namespace: "namespace", Name: "first"}

	// Must be distinct for test to work.
	user1Group1Token := tokenForGroup(user1Group1)
	user1Group2Token := tokenForGroup(user1Group2)
	user2Group1Token := tokenForGroup(user2Group1)
	user3Group1Token := tokenForGroup(user3Group1)

	noRules := map[string]rulespb.RuleGroupList{}
	allRules := map[string]rulespb.RuleGroupList{
		user1: {user1Group1, user1Group2},
		user2: {user2Group1},
		user3: {user3Group1},
	}

	// ruler ID -> (user ID -> list of groups).
	type expectedRulesMap map[string]map[string]rulespb.RuleGroupList

	type testCase struct {
		sharding            bool
		shardingStrategy    string
		replicationFactor   int
		shuffleShardSize    int
		setupRing           func(*ring.Desc)
		enabledUsers        []string
		disabledUsers       []string
		expectedRules       expectedRulesMap
		expectedBackupRules expectedRulesMap
	}

	const (
		ruler1     = "ruler-1"
		ruler1Host = "1.1.1.1"
		ruler1Port = 9999
		ruler1Addr = "1.1.1.1:9999"

		ruler2     = "ruler-2"
		ruler2Host = "2.2.2.2"
		ruler2Port = 9999
		ruler2Addr = "2.2.2.2:9999"

		ruler3     = "ruler-3"
		ruler3Host = "3.3.3.3"
		ruler3Port = 9999
		ruler3Addr = "3.3.3.3:9999"
	)

	testCases := map[string]testCase{
		"no sharding": {
			sharding:          false,
			replicationFactor: 1,
			expectedRules:     expectedRulesMap{ruler1: allRules},
		},

		"no sharding, single user allowed": {
			sharding:          false,
			replicationFactor: 1,
			enabledUsers:      []string{user1},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user1: {user1Group1, user1Group2},
			}},
		},

		"no sharding, single user disabled": {
			sharding:          false,
			replicationFactor: 1,
			disabledUsers:     []string{user1},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user2: {user2Group1},
				user3: {user3Group1},
			}},
		},

		"default sharding, single ruler": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyDefault,
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{ruler1: allRules},
		},

		"default sharding, single ruler, single enabled user": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyDefault,
			enabledUsers:      []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user1: {user1Group1, user1Group2},
			}},
		},

		"default sharding, single ruler, single disabled user": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyDefault,
			disabledUsers:     []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user2: {user2Group1},
				user3: {user3Group1},
			}},
		},

		"default sharding, multiple ACTIVE rulers": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyDefault,
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
					user2: {user2Group1},
				},

				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
					user3: {user3Group1},
				},
			},
		},

		"default sharding, multiple ACTIVE rulers, single enabled user": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyDefault,
			enabledUsers:      []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},

				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
			},
		},

		"default sharding, multiple ACTIVE rulers, single disabled user": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyDefault,
			disabledUsers:     []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
				},

				ruler2: map[string]rulespb.RuleGroupList{
					user3: {user3Group1},
				},
			},
		},

		"default sharding, unhealthy ACTIVE ruler": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyDefault,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.Ingesters[ruler2] = ring.InstanceDesc{
					Addr:      ruler2Addr,
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}),
				}
			},

			expectedRules: expectedRulesMap{
				// This ruler doesn't get rules from unhealthy ruler (RF=1).
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
					user2: {user2Group1},
				},
				ruler2: noRules,
			},
		},

		"default sharding, LEAVING ruler": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyDefault,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.LEAVING, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				// LEAVING ruler doesn't get any rules.
				ruler1: noRules,
				ruler2: allRules,
			},
		},

		"default sharding, JOINING ruler": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyDefault,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.JOINING, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				// JOINING ruler has no rules yet.
				ruler1: noRules,
				ruler2: allRules,
			},
		},

		"shuffle sharding, single ruler": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyShuffle,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{0}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: allRules,
			},
		},

		"shuffle sharding, multiple rulers, shard size 1": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  1,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, userToken(user2, 0) + 1, userToken(user3, 0) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group1Token + 1, user1Group2Token + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: allRules,
				ruler2: noRules,
			},
		},

		// Same test as previous one, but with shard size=2. Second ruler gets all the rules.
		"shuffle sharding, two rulers, shard size 2": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  2,

			setupRing: func(desc *ring.Desc) {
				// Exact same tokens setup as previous test.
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, userToken(user2, 0) + 1, userToken(user3, 0) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group1Token + 1, user1Group2Token + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: noRules,
				ruler2: allRules,
			},
		},

		"shuffle sharding, two rulers, shard size 1, distributed users": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  1,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				ruler2: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},
		"shuffle sharding, three rulers, shard size 2": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  2,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},
				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
				ruler3: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},
		"shuffle sharding, three rulers, shard size 2, ruler2 has no users": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  2,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, userToken(user2, 1) + 1, user1Group1Token + 1, user1Group2Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, userToken(user3, 1) + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				ruler2: noRules, // Ruler2 owns token for user2group1, but user-2 will only be handled by ruler-1 and 3.
				ruler3: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},

		"shuffle sharding, three rulers, shard size 2, single enabled user": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  2,
			enabledUsers:      []string{user1},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},
				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
				ruler3: map[string]rulespb.RuleGroupList{},
			},
		},

		"shuffle sharding, three rulers, shard size 2, single disabled user": {
			sharding:          true,
			replicationFactor: 1,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  2,
			disabledUsers:     []string{user1},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{},
				ruler2: map[string]rulespb.RuleGroupList{},
				ruler3: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},

		"shuffle sharding, three rulers, shard size 2, enable api backup": {
			sharding:          true,
			replicationFactor: 2,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  2,
			enabledUsers:      []string{user1},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},
				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
				ruler3: map[string]rulespb.RuleGroupList{},
			},
			expectedBackupRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},
				ruler3: map[string]rulespb.RuleGroupList{},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			setupRuler := func(id string, host string, port int, forceRing *ring.Ring) *Ruler {
				store := newMockRuleStore(allRules, nil)
				cfg := Config{
					EnableSharding:   tc.sharding,
					ShardingStrategy: tc.shardingStrategy,
					Ring: RingConfig{
						InstanceID:   id,
						InstanceAddr: host,
						InstancePort: port,
						KVStore: kv.Config{
							Mock: kvStore,
						},
						HeartbeatTimeout:  1 * time.Minute,
						ReplicationFactor: tc.replicationFactor,
					},
					FlushCheckPeriod: 0,
					EnabledTenants:   tc.enabledUsers,
					DisabledTenants:  tc.disabledUsers,
				}

				r, _ := buildRuler(t, cfg, nil, store, nil)
				r.limits = ruleLimits{evalDelay: 0, tenantShard: tc.shuffleShardSize}

				if forceRing != nil {
					r.ring = forceRing
				}
				return r
			}

			r1 := setupRuler(ruler1, ruler1Host, ruler1Port, nil)

			rulerRing := r1.ring

			// We start ruler's ring, but nothing else (not even lifecycler).
			if rulerRing != nil {
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), rulerRing))
				t.Cleanup(rulerRing.StopAsync)
			}

			var r2, r3 *Ruler
			if rulerRing != nil {
				// Reuse ring from r1.
				r2 = setupRuler(ruler2, ruler2Host, ruler2Port, rulerRing)
				r3 = setupRuler(ruler3, ruler3Host, ruler3Port, rulerRing)
			}

			if tc.setupRing != nil {
				err := kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
					d, _ := in.(*ring.Desc)
					if d == nil {
						d = ring.NewDesc()
					}

					tc.setupRing(d)

					return d, true, nil
				})
				require.NoError(t, err)
				// Wait a bit to make sure ruler's ring is updated.
				time.Sleep(100 * time.Millisecond)
			}

			// Always add ruler1 to expected rulers, even if there is no ring (no sharding).
			loadedRules1, backupRules1, err := r1.listRules(context.Background())
			require.NoError(t, err)

			expected := expectedRulesMap{
				ruler1: loadedRules1,
			}

			expectedBackup := expectedRulesMap{
				ruler1: backupRules1,
			}

			addToExpected := func(id string, r *Ruler) {
				// Only expect rules from other rulers when using ring, and they are present in the ring.
				if r != nil && rulerRing != nil && rulerRing.HasInstance(id) {
					loaded, backup, err := r.listRules(context.Background())
					require.NoError(t, err)
					// Normalize nil map to empty one.
					if loaded == nil {
						loaded = map[string]rulespb.RuleGroupList{}
					}
					expected[id] = loaded
					expectedBackup[id] = backup
				}
			}

			addToExpected(ruler2, r2)
			addToExpected(ruler3, r3)

			require.Equal(t, tc.expectedRules, expected)

			if tc.replicationFactor <= 1 {
				require.Equal(t, 0, len(expectedBackup[ruler1]))
				require.Equal(t, 0, len(expectedBackup[ruler2]))
				require.Equal(t, 0, len(expectedBackup[ruler3]))
			} else {
				require.Equal(t, tc.expectedBackupRules, expectedBackup)
			}
		})
	}
}

// User shuffle shard token.
func userToken(user string, skip int) uint32 {
	r := rand.New(rand.NewSource(util.ShuffleShardSeed(user, "")))

	for ; skip > 0; skip-- {
		_ = r.Uint32()
	}
	return r.Uint32()
}

func sortTokens(tokens []uint32) []uint32 {
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})
	return tokens
}

func Test_LoadPartialGroups(t *testing.T) {
	const (
		user1 = "user1"
		user2 = "user2"
		user3 = "user3"
	)

	const (
		ruler1     = "ruler-1"
		ruler1Host = "1.1.1.1"
		ruler1Port = 9999
	)

	user1Group1 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "first", Interval: time.Minute}
	user1Group2 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "second", Interval: time.Minute}
	user2Group1 := &rulespb.RuleGroupDesc{User: user2, Namespace: "namespace", Name: "first", Interval: time.Minute}
	user3Group1 := &rulespb.RuleGroupDesc{User: user3, Namespace: "namespace", Name: "first", Interval: time.Minute}

	allRules := map[string]rulespb.RuleGroupList{
		user1: {user1Group1, user1Group2},
		user2: {user2Group1},
		user3: {user3Group1},
	}

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	store := newMockRuleStore(allRules, map[string]error{user1: fmt.Errorf("test")})
	u, _ := url.Parse("")
	cfg := Config{
		EnableSharding:   true,
		ExternalURL:      flagext.URLValue{URL: u},
		PollInterval:     time.Millisecond * 100,
		RingCheckPeriod:  time.Minute,
		ShardingStrategy: util.ShardingStrategyShuffle,
		Ring: RingConfig{
			InstanceID:   ruler1,
			InstanceAddr: ruler1Host,
			InstancePort: ruler1Port,
			KVStore: kv.Config{
				Mock: kvStore,
			},
			HeartbeatTimeout:  1 * time.Minute,
			ReplicationFactor: 1,
		},
		FlushCheckPeriod: 0,
	}

	r1, manager := buildRuler(t, cfg, nil, store, nil)
	r1.limits = ruleLimits{evalDelay: 0, tenantShard: 1}

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r1))
	t.Cleanup(r1.StopAsync)

	err := kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		d, _ := in.(*ring.Desc)
		if d == nil {
			d = ring.NewDesc()
		}
		d.AddIngester(ruler1, fmt.Sprintf("%v:%v", ruler1Host, ruler1Port), "", []uint32{0}, ring.ACTIVE, time.Now())
		return d, true, nil
	})

	require.NoError(t, err)

	test.Poll(t, time.Second*5, true, func() interface{} {
		return len(r1.manager.GetRules(user2)) > 0 &&
			len(r1.manager.GetRules(user3)) > 0
	})

	returned, _, err := r1.listRules(context.Background())
	require.NoError(t, err)
	require.Equal(t, returned, allRules)
	require.Equal(t, 2, len(manager.userManagers))
}

func TestDeleteTenantRuleGroups(t *testing.T) {
	ruleGroups := []ruleGroupKey{
		{user: "userA", namespace: "namespace", group: "group"},
		{user: "userB", namespace: "namespace1", group: "group"},
		{user: "userB", namespace: "namespace2", group: "group"},
	}

	obj, rs := setupRuleGroupsStore(t, ruleGroups)
	require.Equal(t, 3, len(obj.Objects()))

	api, err := NewRuler(Config{}, nil, nil, log.NewNopLogger(), rs, nil)
	require.NoError(t, err)

	{
		req := &http.Request{}
		resp := httptest.NewRecorder()
		api.DeleteTenantConfiguration(resp, req)

		require.Equal(t, http.StatusUnauthorized, resp.Code)
	}

	{
		callDeleteTenantAPI(t, api, "user-with-no-rule-groups")
		require.Equal(t, 3, len(obj.Objects()))

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", false)
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	{
		callDeleteTenantAPI(t, api, "userA")
		require.Equal(t, 2, len(obj.Objects()))

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Just deleted.
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	// Deleting same user again works fine and reports no problems.
	{
		callDeleteTenantAPI(t, api, "userA")
		require.Equal(t, 2, len(obj.Objects()))

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Already deleted before.
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	{
		callDeleteTenantAPI(t, api, "userB")
		require.Equal(t, 0, len(obj.Objects()))

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Deleted previously
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", true)                    // Just deleted
	}
}

func generateTokenForGroups(groups []*rulespb.RuleGroupDesc, offset uint32) []uint32 {
	var tokens []uint32

	for _, g := range groups {
		tokens = append(tokens, tokenForGroup(g)+offset)
	}

	return tokens
}

func callDeleteTenantAPI(t *testing.T, api *Ruler, userID string) {
	ctx := user.InjectOrgID(context.Background(), userID)

	req := &http.Request{}
	resp := httptest.NewRecorder()
	api.DeleteTenantConfiguration(resp, req.WithContext(ctx))

	require.Equal(t, http.StatusOK, resp.Code)
}

func verifyExpectedDeletedRuleGroupsForUser(t *testing.T, r *Ruler, userID string, expectedDeleted bool) {
	list, err := r.store.ListRuleGroupsForUserAndNamespace(context.Background(), userID, "")
	require.NoError(t, err)

	if expectedDeleted {
		require.Equal(t, 0, len(list))
	} else {
		require.NotEqual(t, 0, len(list))
	}
}

func setupRuleGroupsStore(t *testing.T, ruleGroups []ruleGroupKey) (*objstore.InMemBucket, rulestore.RuleStore) {
	bucketClient := objstore.NewInMemBucket()
	rs := bucketclient.NewBucketRuleStore(bucketClient, nil, log.NewNopLogger())

	// "upload" rule groups
	for _, key := range ruleGroups {
		desc := rulespb.ToProto(key.user, key.namespace, rulefmt.RuleGroup{Name: key.group})
		require.NoError(t, rs.SetRuleGroup(context.Background(), key.user, key.namespace, desc))
	}

	return bucketClient, rs
}

type ruleGroupKey struct {
	user, namespace, group string
}

func TestRuler_ListAllRules(t *testing.T) {
	store := newMockRuleStore(mockRules, nil)
	cfg := defaultRulerConfig(t)

	r := newTestRuler(t, cfg, store, nil)
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	router := mux.NewRouter()
	router.Path("/ruler/rule_groups").Methods(http.MethodGet).HandlerFunc(r.ListAllRules)

	req := requestFor(t, http.MethodGet, "https://localhost:8080/ruler/rule_groups", nil, "")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	// Check status code and header
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/yaml", resp.Header.Get("Content-Type"))

	gs := make(map[string]map[string][]rulefmt.RuleGroup) // user:namespace:[]rulefmt.RuleGroup
	for userID := range mockRules {
		gs[userID] = mockRules[userID].Formatted()
	}

	// check for unnecessary fields
	unnecessaryFields := []string{"kind", "style", "tag", "value", "anchor", "alias", "content", "headcomment", "linecomment", "footcomment", "line", "column"}
	for _, word := range unnecessaryFields {
		require.NotContains(t, string(body), word)
	}

	expectedResponse, err := yaml.Marshal(gs)
	require.NoError(t, err)
	require.YAMLEq(t, string(expectedResponse), string(body))
}

type senderFunc func(alerts ...*notifier.Alert)

func (s senderFunc) Send(alerts ...*notifier.Alert) {
	s(alerts...)
}

func TestSendAlerts(t *testing.T) {
	testCases := []struct {
		in  []*promRules.Alert
		exp []*notifier.Alert
	}{
		{
			in: []*promRules.Alert{
				{
					Labels:      []labels.Label{{Name: "l1", Value: "v1"}},
					Annotations: []labels.Label{{Name: "a2", Value: "v2"}},
					ActiveAt:    time.Unix(1, 0),
					FiredAt:     time.Unix(2, 0),
					ValidUntil:  time.Unix(3, 0),
				},
			},
			exp: []*notifier.Alert{
				{
					Labels:       []labels.Label{{Name: "l1", Value: "v1"}},
					Annotations:  []labels.Label{{Name: "a2", Value: "v2"}},
					StartsAt:     time.Unix(2, 0),
					EndsAt:       time.Unix(3, 0),
					GeneratorURL: "http://localhost:9090/graph?g0.expr=up&g0.tab=1",
				},
			},
		},
		{
			in: []*promRules.Alert{
				{
					Labels:      []labels.Label{{Name: "l1", Value: "v1"}},
					Annotations: []labels.Label{{Name: "a2", Value: "v2"}},
					ActiveAt:    time.Unix(1, 0),
					FiredAt:     time.Unix(2, 0),
					ResolvedAt:  time.Unix(4, 0),
				},
			},
			exp: []*notifier.Alert{
				{
					Labels:       []labels.Label{{Name: "l1", Value: "v1"}},
					Annotations:  []labels.Label{{Name: "a2", Value: "v2"}},
					StartsAt:     time.Unix(2, 0),
					EndsAt:       time.Unix(4, 0),
					GeneratorURL: "http://localhost:9090/graph?g0.expr=up&g0.tab=1",
				},
			},
		},
		{
			in: []*promRules.Alert{},
		},
	}

	for i, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			senderFunc := senderFunc(func(alerts ...*notifier.Alert) {
				if len(tc.in) == 0 {
					t.Fatalf("sender called with 0 alert")
				}
				require.Equal(t, tc.exp, alerts)
			})
			SendAlerts(senderFunc, "http://localhost:9090")(context.TODO(), "up", tc.in...)
		})
	}
}

// Tests for whether the Ruler is able to recover ALERTS_FOR_STATE state
func TestRecoverAlertsPostOutage(t *testing.T) {
	// Test Setup
	// alert FOR 30m, already ran for 10m, outage down at 15m prior to now(), outage tolerance set to 1hr
	// EXPECTATION: for state for alert restores to 10m+(now-15m)

	// FIRST set up 1 Alert rule with 30m FOR duration
	alertForDuration, _ := time.ParseDuration("30m")
	mockRules := map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules: []*rulespb.RuleDesc{
					{
						Alert: "UP_ALERT",
						Expr:  "1", // always fire for this test
						For:   alertForDuration,
					},
				},
				Interval: interval,
			},
		},
	}

	// NEXT, set up ruler config with outage tolerance = 1hr
	store := newMockRuleStore(mockRules, nil)
	rulerCfg := defaultRulerConfig(t)
	rulerCfg.OutageTolerance, _ = time.ParseDuration("1h")

	// NEXT, set up mock distributor containing sample,
	// metric: ALERTS_FOR_STATE{alertname="UP_ALERT"}, ts: time.now()-15m, value: time.now()-25m
	currentTime := time.Now().UTC()
	downAtTime := currentTime.Add(time.Minute * -15)
	downAtTimeMs := downAtTime.UnixNano() / int64(time.Millisecond)
	downAtActiveAtTime := currentTime.Add(time.Minute * -25)
	downAtActiveSec := downAtActiveAtTime.Unix()
	d := &querier.MockDistributor{}

	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: []cortexpb.LabelAdapter{
						{Name: labels.MetricName, Value: "ALERTS_FOR_STATE"},
						{Name: labels.AlertName, Value: mockRules["user1"][0].GetRules()[0].Alert},
					},
					Chunks: querier.ConvertToChunks(t, []cortexpb.Sample{{TimestampMs: downAtTimeMs, Value: float64(downAtActiveSec)}}, nil),
				},
			},
		}, nil)
	d.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Panic("This should not be called for the ruler use-cases.")
	querierConfig := querier.DefaultQuerierConfig()

	// set up an empty store
	queryables := []querier.QueryableWithFilter{
		querier.UseAlwaysQueryable(newEmptyQueryable()),
	}

	// create a ruler but don't start it. instead, we'll evaluate the rule groups manually.
	r, _ := buildRuler(t, rulerCfg, &querier.TestConfig{Cfg: querierConfig, Distributor: d, Stores: queryables}, store, nil)
	r.syncRules(context.Background(), rulerSyncReasonInitial)

	// assert initial state of rule group
	ruleGroup := r.manager.GetRules("user1")[0]
	require.Equal(t, time.Time{}, ruleGroup.GetLastEvaluation())
	require.Equal(t, "group1", ruleGroup.Name())
	require.Equal(t, 1, len(ruleGroup.Rules()))

	// assert initial state of rule within rule group
	alertRule := ruleGroup.Rules()[0]
	require.Equal(t, time.Time{}, alertRule.GetEvaluationTimestamp())
	require.Equal(t, "UP_ALERT", alertRule.Name())
	require.Equal(t, promRules.HealthUnknown, alertRule.Health())

	// NEXT, evaluate the rule group the first time and assert
	ctx := user.InjectOrgID(context.Background(), "user1")
	ruleGroup.Eval(ctx, currentTime)

	// since the eval is done at the current timestamp, the activeAt timestamp of alert should equal current timestamp
	require.Equal(t, "UP_ALERT", alertRule.Name())
	require.Equal(t, promRules.HealthGood, alertRule.Health())

	activeMapRaw := reflect.ValueOf(alertRule).Elem().FieldByName("active")
	activeMapKeys := activeMapRaw.MapKeys()
	require.True(t, len(activeMapKeys) == 1)

	activeAlertRuleRaw := activeMapRaw.MapIndex(activeMapKeys[0]).Elem()
	activeAtTimeRaw := activeAlertRuleRaw.FieldByName("ActiveAt")

	require.Equal(t, promRules.StatePending, promRules.AlertState(activeAlertRuleRaw.FieldByName("State").Int()))
	require.Equal(t, reflect.NewAt(activeAtTimeRaw.Type(), unsafe.Pointer(activeAtTimeRaw.UnsafeAddr())).Elem().Interface().(time.Time), currentTime)

	// NEXT, restore the FOR state and assert
	ruleGroup.RestoreForState(currentTime)

	require.Equal(t, "UP_ALERT", alertRule.Name())
	require.Equal(t, promRules.HealthGood, alertRule.Health())
	require.Equal(t, promRules.StatePending, promRules.AlertState(activeAlertRuleRaw.FieldByName("State").Int()))
	require.Equal(t, reflect.NewAt(activeAtTimeRaw.Type(), unsafe.Pointer(activeAtTimeRaw.UnsafeAddr())).Elem().Interface().(time.Time), downAtActiveAtTime.Add(currentTime.Sub(downAtTime)))

	// NEXT, 20 minutes is expected to be left, eval timestamp at currentTimestamp +20m
	currentTime = currentTime.Add(time.Minute * 20)
	ruleGroup.Eval(ctx, currentTime)

	// assert alert state after alert is firing
	firedAtRaw := activeAlertRuleRaw.FieldByName("FiredAt")
	firedAtTime := reflect.NewAt(firedAtRaw.Type(), unsafe.Pointer(firedAtRaw.UnsafeAddr())).Elem().Interface().(time.Time)
	require.Equal(t, firedAtTime, currentTime)

	require.Equal(t, promRules.StateFiring, promRules.AlertState(activeAlertRuleRaw.FieldByName("State").Int()))
}

func TestRulerDisablesRuleGroups(t *testing.T) {
	const (
		ruler1     = "ruler-1"
		ruler1Host = "1.1.1.1"
		ruler1Port = 9999
		ruler1Addr = "1.1.1.1:9999"

		ruler2     = "ruler-2"
		ruler2Host = "2.2.2.2"
		ruler2Port = 9999
		ruler2Addr = "2.2.2.2:9999"

		ruler3     = "ruler-3"
		ruler3Host = "3.3.3.3"
		ruler3Port = 9999
		ruler3Addr = "3.3.3.3:9999"
	)
	const (
		user1 = "user1"
		user2 = "user2"
		user3 = "user3"
	)

	user1Group1 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace1", Name: "group1"}
	user1Group2 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace1", Name: "group2"}
	user2Group1 := &rulespb.RuleGroupDesc{User: user2, Namespace: "namespace1", Name: "group1"}
	user3Group1 := &rulespb.RuleGroupDesc{User: user3, Namespace: "namespace1", Name: "group1"}

	user1Group1Token := tokenForGroup(user1Group1)
	user1Group2Token := tokenForGroup(user1Group2)
	user2Group1Token := tokenForGroup(user2Group1)
	user3Group1Token := tokenForGroup(user3Group1)

	d := &querier.MockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Chunks: []client.Chunk{},
				},
			},
		}, nil)

	ruleGroupDesc := func(user, name, namespace string) *rulespb.RuleGroupDesc {
		return &rulespb.RuleGroupDesc{
			Name:      name,
			Namespace: namespace,
			User:      user,
		}
	}

	ruleGroupWithRule := func(expr, user, name, namespace string) *rulespb.RuleGroupDesc {
		rg := ruleGroupDesc(user, name, namespace)
		rg.Rules = []*rulespb.RuleDesc{
			{
				Record: "RecordingRule",
				Expr:   expr,
			},
		}
		return rg
	}

	disabledRuleGroups := validation.DisabledRuleGroups{
		validation.DisabledRuleGroup{
			Namespace: "namespace1",
			Name:      "group1",
			User:      "user1",
		},
	}

	for _, tc := range []struct {
		name                      string
		rules                     map[string]rulespb.RuleGroupList
		expectedRuleGroupsForUser map[string]rulespb.RuleGroupList
		sharding                  bool
		shardingStrategy          string
		setupRing                 func(*ring.Desc)
		disabledRuleGroups        validation.DisabledRuleGroups
	}{
		{
			name: "disables rule group - shuffle sharding",
			rules: map[string]rulespb.RuleGroupList{
				"user1": {ruleGroupWithRule("up[240m:1s]", "user1", "group1", "namespace1")},
				"user2": {ruleGroupWithRule("up[240m:1s]", "user2", "group1", "namespace1")},
			},
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			expectedRuleGroupsForUser: map[string]rulespb.RuleGroupList{
				"user2": {ruleGroupDesc("user2", "group1", "namespace1")},
			},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},
			disabledRuleGroups: disabledRuleGroups,
		},
		{
			name: "disables rule group - no sharding",
			rules: map[string]rulespb.RuleGroupList{
				"user1": {ruleGroupWithRule("up[240m:1s]", "user1", "group1", "namespace1")},
				"user2": {ruleGroupWithRule("up[240m:1s]", "user2", "group1", "namespace1")},
			},
			sharding: false,
			expectedRuleGroupsForUser: map[string]rulespb.RuleGroupList{
				"user2": {ruleGroupDesc("user2", "group1", "namespace1")},
			},
			disabledRuleGroups: disabledRuleGroups,
		},
		{
			name: "disables rule group - default sharding",
			rules: map[string]rulespb.RuleGroupList{
				"user1": {ruleGroupWithRule("up[240m:1s]", "user1", "group1", "namespace1")},
				"user2": {ruleGroupWithRule("up[240m:1s]", "user2", "group1", "namespace1")},
			},
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			expectedRuleGroupsForUser: map[string]rulespb.RuleGroupList{
				"user2": {ruleGroupDesc("user2", "group1", "namespace1")},
			},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},
			disabledRuleGroups: disabledRuleGroups,
		},
		{
			name: "disables rule group - default sharding",
			rules: map[string]rulespb.RuleGroupList{
				"user1": {ruleGroupWithRule("up[240m:1s]", "user1", "group1", "namespace1")},
				"user2": {ruleGroupWithRule("up[240m:1s]", "user2", "group1", "namespace1")},
			},
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			expectedRuleGroupsForUser: map[string]rulespb.RuleGroupList{
				"user1": {ruleGroupDesc("user1", "group1", "namespace1")},
				"user2": {ruleGroupDesc("user2", "group1", "namespace1")},
			},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			setupRuler := func(id string, host string, port int, forceRing *ring.Ring) *Ruler {
				store := newMockRuleStore(tc.rules, nil)
				cfg := Config{
					EnableSharding:   tc.sharding,
					ShardingStrategy: tc.shardingStrategy,
					Ring: RingConfig{
						InstanceID:   id,
						InstanceAddr: host,
						InstancePort: port,
						KVStore: kv.Config{
							Mock: kvStore,
						},
						HeartbeatTimeout:  1 * time.Minute,
						ReplicationFactor: 1,
					},
					FlushCheckPeriod: 0,
				}

				r, _ := buildRuler(t, cfg, nil, store, nil)
				r.limits = ruleLimits{evalDelay: 0, tenantShard: 3, disabledRuleGroups: tc.disabledRuleGroups}

				if forceRing != nil {
					r.ring = forceRing
				}
				return r
			}

			r1 := setupRuler(ruler1, ruler1Host, ruler1Port, nil)

			rulerRing := r1.ring

			// We start ruler's ring, but nothing else (not even lifecycler).
			if rulerRing != nil {
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), rulerRing))
				t.Cleanup(rulerRing.StopAsync)
			}

			var r2, r3 *Ruler
			if rulerRing != nil {
				// Reuse ring from r1.
				r2 = setupRuler(ruler2, ruler2Host, ruler2Port, rulerRing)
				r3 = setupRuler(ruler3, ruler3Host, ruler3Port, rulerRing)
			}

			if tc.setupRing != nil {
				err := kvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
					d, _ := in.(*ring.Desc)
					if d == nil {
						d = ring.NewDesc()
					}

					tc.setupRing(d)

					return d, true, nil
				})
				require.NoError(t, err)
				// Wait a bit to make sure ruler's ring is updated.
				time.Sleep(100 * time.Millisecond)
			}

			actualRules := map[string]rulespb.RuleGroupList{}
			loadedRules, _, err := r1.listRules(context.Background())
			require.NoError(t, err)
			for k, v := range loadedRules {
				if len(v) > 0 {
					actualRules[k] = v
				}
			}

			fetchRules := func(id string, r *Ruler) {
				// Only expect rules from other rulers when using ring, and they are present in the ring.
				if r != nil && rulerRing != nil && rulerRing.HasInstance(id) {
					loaded, _, err := r.listRules(context.Background())
					require.NoError(t, err)

					// Normalize nil map to empty one.
					if loaded == nil {
						loaded = map[string]rulespb.RuleGroupList{}
					}
					for k, v := range loaded {
						actualRules[k] = v
					}
				}
			}

			fetchRules(ruler2, r2)
			fetchRules(ruler3, r3)

			require.Equal(t, tc.expectedRuleGroupsForUser, actualRules)
		})
	}
}
