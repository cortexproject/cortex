package ruler

import (
	"context"
	"encoding/binary"
	"fmt"
	io "io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/ha"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore/bucketclient"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/purger"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
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
	cfg.EnableQueryStats = false
	cfg.PollInterval = 10 * time.Second

	return cfg
}

type ruleLimits struct {
	evalDelay            time.Duration
	tenantShard          int
	maxRulesPerRuleGroup int
	maxRuleGroups        int
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

func newEmptyQueryable() storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return emptyQuerier{}, nil
	})
}

type emptyQuerier struct {
}

func (e emptyQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (e emptyQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (e emptyQuerier) Close() error {
	return nil
}

func (e emptyQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}

func testQueryableFunc(querierTestConfig *querier.TestConfig, reg prometheus.Registerer, logger log.Logger) storage.QueryableFunc {
	if querierTestConfig != nil {
		// disable active query tracking for test
		querierTestConfig.Cfg.ActiveQueryTrackerDir = ""

		overrides, _ := validation.NewOverrides(querier.DefaultLimitsConfig(), nil)
		q, _, _ := querier.New(querierTestConfig.Cfg, overrides, querierTestConfig.Distributor, querierTestConfig.Stores, purger.NewNoopTombstonesLoader(), reg, logger)
		return func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			return q.Querier(ctx, mint, maxt)
		}
	}

	return func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
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
	manager, err := NewDefaultMultiTenantManager(cfg, DefaultTenantManagerFactory(cfg, pusher, queryable, engine, overrides, nil), reg, logger)
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

func buildRuler(t *testing.T, rulerConfig Config, querierTestConfig *querier.TestConfig, store rulestore.RuleStore, rulerAddrMap map[string]*Ruler) *Ruler {
	return buildRulerWithCustomGroupHash(t, rulerConfig, querierTestConfig, store, rulerAddrMap, tokenForGroup)
}

func buildRulerWithCustomGroupHash(t *testing.T, rulerConfig Config, querierTestConfig *querier.TestConfig, store rulestore.RuleStore, rulerAddrMap map[string]*Ruler, groupHash RuleGroupHashFunc) *Ruler {
	engine, queryable, pusher, logger, overrides, reg := testSetup(t, querierTestConfig)

	managerFactory := DefaultTenantManagerFactory(rulerConfig, pusher, queryable, engine, overrides, reg)
	manager, err := NewDefaultMultiTenantManager(rulerConfig, managerFactory, reg, logger)
	require.NoError(t, err)

	ruler, err := newRuler(
		rulerConfig,
		manager,
		reg,
		logger,
		store,
		overrides,
		newMockClientsPool(rulerConfig, logger, reg, rulerAddrMap),
		groupHash,
	)
	require.NoError(t, err)
	return ruler
}

func newTestRuler(t *testing.T, rulerConfig Config, store rulestore.RuleStore, querierTestConfig *querier.TestConfig) *Ruler {
	ruler := buildRuler(t, rulerConfig, querierTestConfig, store, nil)
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
	store := newMockRuleStore(mockRules)
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

func TestGetRules_filtering(t *testing.T) {
	// ruler ID -> (user ID -> list of groups).
	type expectedRulesMap map[string]map[string]rulespb.RuleGroupList
	type rulesMap map[string][]*rulespb.RuleDesc

	type testCase struct {
		sharding          bool
		shardingStrategy  string
		shuffleShardSize  int
		replicationFactor int
		rulesRequest      RulesRequest
		expectedCount     map[string]int
	}

	ruleMap := rulesMap{
		"ruler1-user1-rule-group1": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
		"ruler1-user1-rule-group2": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
		"ruler1-user2-rule-group1": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
		"ruler2-user1-rule-group3": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
		"ruler2-user2-rule-group1": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
		"ruler2-user2-rule-group2": []*rulespb.RuleDesc{
			{
				Record: "rtest_user2_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
			{
				Alert: "atest_user2_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
		"ruler2-user3-rule-group1": []*rulespb.RuleDesc{
			{
				Alert: "atest_user3_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
		"ruler3-user2-rule-group1": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
		"ruler3-user2-rule-group2": []*rulespb.RuleDesc{
			{
				Record: "rtest_user1_1",
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
		"ruler3-user3-rule-group1": []*rulespb.RuleDesc{
			{
				Expr:   "sum(rate(node_cpu_seconds_total[3h:10m]))",
				Record: "rtest_user1_1",
			},
			{
				Alert: "atest_user1_1",
				Expr:  "sum(rate(node_cpu_seconds_total[3h:10m]))",
			},
		},
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
			expectedCount: map[string]int{
				"user1": 2,
				"user2": 4,
				"user3": 2,
			},
		},
		"Default Sharding with No Filter": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			expectedCount: map[string]int{
				"user1": 5,
				"user2": 9,
				"user3": 3,
			},
		},
		"Default Sharding and replicationFactor = 3 with No Filter": {
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			expectedCount: map[string]int{
				"user1": 5,
				"user2": 9,
				"user3": 3,
			},
		},
		"Shuffle Sharding and ShardSize = 2 with Rule Type Filter": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulesRequest: RulesRequest{
				Type: recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 3,
				"user2": 5,
				"user3": 1,
			},
		},
		"Shuffle Sharding and ShardSize = 2 and Rule Group Name Filter": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulesRequest: RulesRequest{
				RuleGroupNames: []string{"third"},
			},
			expectedCount: map[string]int{
				"user1": 2,
				"user2": 1,
				"user3": 2,
			},
		},
		"Shuffle Sharding and ShardSize = 2 and Rule Group Name and Rule Type Filter": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulesRequest: RulesRequest{
				RuleGroupNames: []string{"second", "third"},
				Type:           recordingRuleFilter,
			},
			expectedCount: map[string]int{
				"user1": 2,
				"user2": 2,
				"user3": 1,
			},
		},
		"Shuffle Sharding and ShardSize = 2 with Rule Type and Namespace Filters": {
			sharding:         true,
			shuffleShardSize: 2,
			shardingStrategy: util.ShardingStrategyShuffle,
			rulesRequest: RulesRequest{
				Type:  alertingRuleFilter,
				Files: []string{"latency-test"},
			},
			expectedCount: map[string]int{
				"user1": 0,
				"user2": 0,
				"user3": 1,
			},
		},
		"Shuffle Sharding and ShardSize = 3 and replicationFactor = 3 with No Filter": {
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  3,
			replicationFactor: 3,
			expectedCount: map[string]int{
				"user1": 5,
				"user2": 9,
				"user3": 3,
			},
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
				store := newMockRuleStore(allRulesByUser)
				cfg := defaultRulerConfig(t)

				cfg.ShardingStrategy = tc.shardingStrategy
				cfg.EnableSharding = tc.sharding

				rulerReplicationFactor := tc.replicationFactor
				if rulerReplicationFactor == 0 {
					rulerReplicationFactor = 1
				}

				cfg.Ring = RingConfig{
					InstanceID:   id,
					InstanceAddr: id,
					KVStore: kv.Config{
						Mock: kvStore,
					},
					ReplicationFactor: rulerReplicationFactor,
				}

				r := buildRuler(t, cfg, nil, store, rulerAddrMap)
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
						d.AddIngester(rID, rulerAddrMap[rID].lifecycler.GetInstanceAddr(), "", tokens, ring.ACTIVE, time.Now())
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
			for u := range allRulesByUser {
				ctx := user.InjectOrgID(context.Background(), u)
				forEachRuler(func(_ string, r *Ruler) {
					ruleStateDescriptions, err := r.GetRules(ctx, Weak, tc.rulesRequest)
					require.NoError(t, err)
					rct := 0
					for _, ruleStateDesc := range ruleStateDescriptions {
						rct += len(ruleStateDesc.ActiveRules)
					}
					require.Equal(t, tc.expectedCount[u], rct)
					// If replication factor larger than 1, we don't necessary need to wait for all ruler's call to complete to get the
					// complete result
					if tc.sharding && tc.replicationFactor <= 1 {
						mockPoolClient := r.clientsPool.(*mockRulerClientsPool)

						if tc.shardingStrategy == util.ShardingStrategyShuffle {
							require.Equal(t, int32(tc.shuffleShardSize), mockPoolClient.numberOfCalls.Load())
						} else {
							require.Equal(t, int32(len(rulerAddrMap)), mockPoolClient.numberOfCalls.Load())
						}
						mockPoolClient.numberOfCalls.Store(0)
					}
				})
			}

			totalLoadedRules := 0
			totalConfiguredRules := 0

			forEachRuler(func(rID string, r *Ruler) {
				localRules, err := r.listRules(context.Background())
				require.NoError(t, err)
				for _, rules := range localRules {
					totalLoadedRules += len(rules)
				}
				totalConfiguredRules += len(allRulesByRuler[rID])
			})

			if tc.sharding && tc.replicationFactor <= 1 {
				require.Equal(t, totalConfiguredRules, totalLoadedRules)
			} else if tc.replicationFactor > 1 {
				require.Equal(t, totalConfiguredRules*tc.replicationFactor, totalLoadedRules)
			} else {
				// Not sharding means that all rules will be loaded on all rulers
				numberOfRulers := len(rulerAddrMap)
				require.Equal(t, totalConfiguredRules*numberOfRulers, totalLoadedRules)
			}
		})
	}
}

func TestGetRules_quorum(t *testing.T) {
	// ruler ID -> (user ID -> list of groups).
	type singleRulerConfig map[string]rulespb.RuleGroupList
	type multiRulerConfig map[string]singleRulerConfig

	// We coerce rules to have categories of evaluation timestamps by assigning progressively larger
	// group evaluation intervals, and waiting long enough for evaluations to trigger in the desired order.
	newestEval := 1 * time.Second
	newEval := 5 * time.Second
	oldEval := 30 * time.Second

	// overriding the ruler token gives us control over which rulers own which rulegroups
	getRulerToken := func(rulerId string) uint32 {
		index, _ := strconv.ParseUint(rulerId[5:], 10, 8)
		return uint32(index * 1000)
	}

	// we encode rule groups as X, X+, X++ to denote the same rule group with old, newer,
	// and newest evaluation timestamps, respectively.  Encoding it like this enables us to express lists
	// of rule groups in a compact form.
	encodeExpectedGroup := func(rg *rulespb.RuleGroupDesc) (string, error) {
		if rg.Interval == oldEval {
			return rg.Name, nil
		} else if rg.Interval == newEval {
			return rg.Name + "+", nil
		} else if rg.Interval == newestEval {
			return rg.Name + "++", nil
		} else {
			return "", errors.Errorf("Unexpected interval: %s", rg.Interval.String())
		}
	}
	decodeExpectedGroup := func(user string, rgstr string, token uint32) *rulespb.RuleGroupDesc {
		interval = oldEval
		if rgstr[1:] == "++" {
			interval = newestEval
		} else if rgstr[1:] == "+" {
			interval = newEval
		}

		rules := []*rulespb.RuleDesc{
			{
				Expr:   "count(up)",
				Record: fmt.Sprintf("%s_0", string(rgstr[0])),
			},
		}

		tokenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(tokenBytes, token)

		return &rulespb.RuleGroupDesc{
			User:      user,
			Namespace: "namespace",
			Name:      string(rgstr[0]),
			Interval:  interval,
			Rules:     rules,
			Options: []*types.Any{
				{
					TypeUrl: "dummy",
					Value:   tokenBytes,
				},
			},
		}
	}

	newRuleGroups := func(rulerId string, user string, encodedRuleGroups []string) rulespb.RuleGroupList {
		rulegroups := make(rulespb.RuleGroupList, len(encodedRuleGroups))
		ruleToken := getRulerToken(rulerId) - 1 // this ensures this rule will be owned by ruler rulerID
		for i, encodedRuleGroup := range encodedRuleGroups {
			rulegroups[i] = decodeExpectedGroup(user, encodedRuleGroup, ruleToken)
		}
		return rulegroups
	}
	addRuleGroup := func(expectedRules multiRulerConfig, rulerId string, user string, encodedRuleGroups []string) {
		_, ok := expectedRules[rulerId]
		if !ok {
			expectedRules[rulerId] = make(singleRulerConfig)
		}
		expectedRules[rulerId][user] = newRuleGroups(rulerId, user, encodedRuleGroups)
	}

	t.Logf("Building expectedRules")
	expectedRules := make(multiRulerConfig)
	rulerID := "ruler1"
	addRuleGroup(expectedRules, rulerID, "user0", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user1", []string{"A"})
	addRuleGroup(expectedRules, rulerID, "user2", []string{"A"})
	addRuleGroup(expectedRules, rulerID, "user3", []string{"A", "F"})
	addRuleGroup(expectedRules, rulerID, "user4", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user5", []string{"A+", "F+"})
	addRuleGroup(expectedRules, rulerID, "user6", []string{"A", "F", "E"})
	addRuleGroup(expectedRules, rulerID, "user7", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user8", []string{"A+", "F+", "E+"})
	addRuleGroup(expectedRules, rulerID, "user9", []string{"A+", "F+", "E+"})
	addRuleGroup(expectedRules, rulerID, "user11", []string{"A"})
	addRuleGroup(expectedRules, rulerID, "user12", []string{"A"})
	addRuleGroup(expectedRules, rulerID, "user13", []string{"A", "F"})
	addRuleGroup(expectedRules, rulerID, "user14", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user15", []string{"A+", "F+"})
	addRuleGroup(expectedRules, rulerID, "user16", []string{"A", "F", "E"})
	addRuleGroup(expectedRules, rulerID, "user17", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user18", []string{"A+", "F+", "E+"})
	addRuleGroup(expectedRules, rulerID, "user19", []string{"A+", "F+", "E+"})
	rulerID = "ruler2"
	addRuleGroup(expectedRules, rulerID, "user1", []string{"A", "B", "C", "D", "E", "F"})
	// user2 has no rules in ruler 2
	addRuleGroup(expectedRules, rulerID, "user3", []string{"B", "A"})
	addRuleGroup(expectedRules, rulerID, "user4", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user5", []string{"B+", "A+"})
	addRuleGroup(expectedRules, rulerID, "user6", []string{"B", "A", "F"})
	addRuleGroup(expectedRules, rulerID, "user7", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user8", []string{"B+", "A+", "F+"})
	addRuleGroup(expectedRules, rulerID, "user9", []string{"B+", "A+", "F++"})
	addRuleGroup(expectedRules, rulerID, "user11", []string{"A", "B", "C", "D", "E", "F"})
	// user12 has no rules in ruler 2
	addRuleGroup(expectedRules, rulerID, "user13", []string{"B", "A"})
	addRuleGroup(expectedRules, rulerID, "user14", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user15", []string{"B+", "A+"})
	addRuleGroup(expectedRules, rulerID, "user16", []string{"B", "A", "F"})
	addRuleGroup(expectedRules, rulerID, "user17", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user18", []string{"B+", "A+", "F+"})
	addRuleGroup(expectedRules, rulerID, "user19", []string{"B+", "A+", "F++"})
	rulerID = "ruler3"
	addRuleGroup(expectedRules, rulerID, "user1", []string{"C"})
	addRuleGroup(expectedRules, rulerID, "user2", []string{"B"})
	addRuleGroup(expectedRules, rulerID, "user3", []string{"C", "B"})
	//// user4 has no rules in ruler3
	addRuleGroup(expectedRules, rulerID, "user5", []string{"C+", "B+"})
	addRuleGroup(expectedRules, rulerID, "user6", []string{"C", "B", "A"})
	addRuleGroup(expectedRules, rulerID, "user7", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user8", []string{"C+", "B+", "A+"})
	addRuleGroup(expectedRules, rulerID, "user9", []string{"C+", "B+", "A+"})
	addRuleGroup(expectedRules, rulerID, "user11", []string{"C"})
	addRuleGroup(expectedRules, rulerID, "user12", []string{"B"})
	addRuleGroup(expectedRules, rulerID, "user13", []string{"C", "B"})
	//// user14 has no rules in ruler3
	addRuleGroup(expectedRules, rulerID, "user15", []string{"C+", "B+"})
	addRuleGroup(expectedRules, rulerID, "user16", []string{"C", "B", "A"})
	addRuleGroup(expectedRules, rulerID, "user17", []string{"A", "B", "C"})
	addRuleGroup(expectedRules, rulerID, "user18", []string{"C+", "B+", "A+"})
	addRuleGroup(expectedRules, rulerID, "user19", []string{"C+", "B+", "A+"})
	rulerID = "ruler4"
	addRuleGroup(expectedRules, rulerID, "user1", []string{"D"})
	// user2 has no rules in ruler 4
	addRuleGroup(expectedRules, rulerID, "user3", []string{"D", "C"})
	//// user4 has no rules in ruler4
	addRuleGroup(expectedRules, rulerID, "user5", []string{"D", "C"})
	addRuleGroup(expectedRules, rulerID, "user6", []string{"D", "C", "B"})
	addRuleGroup(expectedRules, rulerID, "user7", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user8", []string{"D", "C", "B"})
	addRuleGroup(expectedRules, rulerID, "user9", []string{"D", "C", "B"})
	addRuleGroup(expectedRules, rulerID, "user11", []string{"D"})
	// user12 has no rules in ruler 4
	addRuleGroup(expectedRules, rulerID, "user13", []string{"D", "C"})
	//// user14 has no rules in ruler4
	addRuleGroup(expectedRules, rulerID, "user15", []string{"D", "C"})
	addRuleGroup(expectedRules, rulerID, "user16", []string{"D", "C", "B"})
	addRuleGroup(expectedRules, rulerID, "user17", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user18", []string{"D", "C", "B"})
	addRuleGroup(expectedRules, rulerID, "user19", []string{"D", "C", "B"})
	rulerID = "ruler5"
	addRuleGroup(expectedRules, rulerID, "user1", []string{"E"})
	addRuleGroup(expectedRules, rulerID, "user2", []string{"C"})
	addRuleGroup(expectedRules, rulerID, "user3", []string{"E", "D"})
	addRuleGroup(expectedRules, rulerID, "user4", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user5", []string{"E", "D"})
	addRuleGroup(expectedRules, rulerID, "user6", []string{"E", "D", "C"})
	addRuleGroup(expectedRules, rulerID, "user7", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user8", []string{"E", "D", "C"})
	addRuleGroup(expectedRules, rulerID, "user9", []string{"E", "D", "C"})
	addRuleGroup(expectedRules, rulerID, "user11", []string{"E"})
	addRuleGroup(expectedRules, rulerID, "user12", []string{"C"})
	addRuleGroup(expectedRules, rulerID, "user13", []string{"E", "D"})
	addRuleGroup(expectedRules, rulerID, "user14", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user15", []string{"E", "D"})
	addRuleGroup(expectedRules, rulerID, "user16", []string{"E", "D", "C"})
	addRuleGroup(expectedRules, rulerID, "user17", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user18", []string{"E", "D", "C"})
	addRuleGroup(expectedRules, rulerID, "user19", []string{"E", "D", "C"})
	rulerID = "ruler6"
	addRuleGroup(expectedRules, rulerID, "user1", []string{"F"})
	// user2 has no rules in ruler 6
	addRuleGroup(expectedRules, rulerID, "user3", []string{"F", "E"})
	addRuleGroup(expectedRules, rulerID, "user4", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user5", []string{"F", "E"})
	addRuleGroup(expectedRules, rulerID, "user6", []string{"F", "E", "D"})
	addRuleGroup(expectedRules, rulerID, "user7", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user8", []string{"F", "E", "D"})
	addRuleGroup(expectedRules, rulerID, "user9", []string{"F", "E", "D"})
	addRuleGroup(expectedRules, rulerID, "user11", []string{"F"})
	// user12 has no rules in ruler 6
	addRuleGroup(expectedRules, rulerID, "user13", []string{"F", "E"})
	addRuleGroup(expectedRules, rulerID, "user14", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user15", []string{"F", "E"})
	addRuleGroup(expectedRules, rulerID, "user16", []string{"F", "E", "D"})
	addRuleGroup(expectedRules, rulerID, "user17", []string{"D", "E", "F"})
	addRuleGroup(expectedRules, rulerID, "user18", []string{"F", "E", "D"})
	addRuleGroup(expectedRules, rulerID, "user19", []string{"F", "E", "D"})
	// ruler7 is used tp confirm that shardSize=6 does not return rules from outside the shard
	rulerID = "ruler7"
	addRuleGroup(expectedRules, rulerID, "user11", []string{"Z"})
	addRuleGroup(expectedRules, rulerID, "user12", []string{"Y"})
	addRuleGroup(expectedRules, rulerID, "user13", []string{"X"})
	addRuleGroup(expectedRules, rulerID, "user14", []string{"W"})
	addRuleGroup(expectedRules, rulerID, "user15", []string{"V"})
	addRuleGroup(expectedRules, rulerID, "user16", []string{"U"})
	addRuleGroup(expectedRules, rulerID, "user17", []string{"T"})
	addRuleGroup(expectedRules, rulerID, "user18", []string{"S"})
	addRuleGroup(expectedRules, rulerID, "user19", []string{"R"})

	type testCase struct {
		name string

		// ruler configuration parameters
		sharding          bool
		shardingStrategy  string
		shuffleShardSize  int
		replicationFactor int
		unavailableRulers []string

		// request and test parameters
		quorum         QuorumType
		rulerID        string
		user           string
		expectedGroups []string
		expectedError  string
	}

	// testCase helper functions for sorting, to minimize ruler reconfigurations
	testCaseConfigEqual := func(a testCase, b testCase) bool {
		return a.sharding == b.sharding &&
			a.shardingStrategy == b.shardingStrategy &&
			a.shuffleShardSize == b.shuffleShardSize &&
			a.replicationFactor == b.replicationFactor &&
			strings.Join(a.unavailableRulers, ",") == strings.Join(b.unavailableRulers, ",")
	}
	testCaseConfigLess := func(a testCase, b testCase) bool {
		if a.sharding == b.sharding {
			if a.shardingStrategy == b.shardingStrategy {
				if a.shuffleShardSize == b.shuffleShardSize {
					if a.replicationFactor == b.replicationFactor {
						return strings.Join(a.unavailableRulers, ",") < strings.Join(b.unavailableRulers, ",")
					}
					return a.replicationFactor < b.replicationFactor
				}
				return a.shuffleShardSize < b.shuffleShardSize
			}
			return a.shardingStrategy < b.shardingStrategy
		}
		return a.sharding == false
	}

	t.Logf("Building testCases")
	testCases := []testCase{
		{
			name:              "1 No Sharding, weak quorum",
			sharding:          false,
			replicationFactor: 1,
			rulerID:           "ruler1",
			user:              "user0",
			expectedGroups:    []string{"A", "B", "C"},
		},
		{
			name:              "2 Default Sharding, weak quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			user:              "user1",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "3 Default Sharding, weak quorum, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			user:              "user2",
			expectedGroups:    []string{"A", "B", "C"},
		},
		{
			name:              "4 Default Sharding, replicationFactor = 2, weak quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			user:              "user3",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "5 Default Sharding, replicationFactor = 2, weak quorum, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			user:              "user4",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "6 Default Sharding, replicationFactor = 2, weak quorum, disagreement",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			user:              "user5",
			expectedGroups:    []string{"A+", "B+", "C+", "D", "E", "F+"},
		},
		{
			name:              "7 Default Sharding, replicationFactor = 2, weak quorum, disagreement due to unavailability",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			user:              "user3",
			unavailableRulers: []string{"ruler3"},
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "8 Default Sharding, replicationFactor = 3, weak quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			user:              "user6",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "9 Default Sharding, replicationFactor = 3, weak quorum, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			user:              "user7",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "10 Default Sharding, replicationFactor = 3, weak quorum, disagreement with quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			user:              "user8",
			expectedGroups:    []string{"A+", "B+", "C", "D", "E", "F+"},
		},
		{
			name:              "11 Default Sharding, replicationFactor = 3, weak quorum, unavailability with quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			user:              "user6",
			unavailableRulers: []string{"ruler3"},
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "12 Default Sharding, replicationFactor = 3, weak quorum, disagreement no quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			user:              "user9",
			expectedGroups:    []string{"A+", "B+", "C", "D", "E", "F++"},
		},
		{
			name:              "13 Default Sharding, replicationFactor = 3, weak quorum, unavailability no quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			user:              "user6",
			unavailableRulers: []string{"ruler3", "ruler4"},
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "14 Shuffle Sharding, ShardSize = 6, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 1,
			user:              "user11",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "15 Shuffle Sharding, ShardSize = 6, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 1,
			user:              "user12",
			expectedGroups:    []string{"A", "B", "C"},
		},
		{
			name:              "16 Shuffle Sharding, ShardSize = 6, replicationFactor = 2, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 2,
			user:              "user13",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "17 Shuffle Sharding, ShardSize = 6, replicationFactor = 2, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 2,
			user:              "user14",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "18 Shuffle Sharding, ShardSize = 6, replicationFactor = 2, disagreement",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 2,
			user:              "user15",
			expectedGroups:    []string{"A+", "B+", "C+", "D", "E", "F+"},
		},
		{
			name:              "19 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, weak quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			user:              "user16",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "20 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, weak quorum, split",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			user:              "user17",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "21 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, weak quorum, disagreement with quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			user:              "user18",
			expectedGroups:    []string{"A+", "B+", "C", "D", "E", "F+"},
		},
		{
			name:              "22 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, weak quorum, unavailability with quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			user:              "user16",
			unavailableRulers: []string{"ruler3"},
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "23 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, weak quorum, disagreement no quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			user:              "user19",
			expectedGroups:    []string{"A+", "B+", "C", "D", "E", "F++"},
		},
		{
			name:              "24 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, weak quorum, unavailability no quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			user:              "user16",
			unavailableRulers: []string{"ruler3", "ruler4"},
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},

		{
			name:              "25 No Sharding, strong quorum",
			sharding:          false,
			replicationFactor: 1,
			quorum:            Strong,
			rulerID:           "ruler1",
			user:              "user0",
			expectedGroups:    []string{"A", "B", "C"},
		},
		{
			name:              "26 Default Sharding, strong quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			quorum:            Strong,
			user:              "user1",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "27 Default Sharding, strong quorum, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			quorum:            Strong,
			user:              "user2",
			expectedGroups:    []string{"A", "B", "C"},
		},
		{
			name:              "28 Default Sharding, replicationFactor = 2, strong quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			quorum:            Strong,
			user:              "user3",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "29 Default Sharding, replicationFactor = 2, strong quorum, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			quorum:            Strong,
			user:              "user4",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "30 Default Sharding, replicationFactor = 2, strong quorum, disagreement",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			quorum:            Strong,
			user:              "user5",
			expectedGroups:    []string{"A+", "B+", "C+", "D", "E", "F+"},
		},
		{
			name:              "31 Default Sharding, replicationFactor = 2, strong quorum, disagreement due to unavailability",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			quorum:            Strong,
			user:              "user3",
			unavailableRulers: []string{"ruler3"},
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "32 Default Sharding, replicationFactor = 3, strong quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user6",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "33 Default Sharding, replicationFactor = 3, strong quorum, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user7",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "34 Default Sharding, replicationFactor = 3, strong quorum, disagreement with quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user8",
			expectedGroups:    []string{"A+", "B+", "C", "D", "E", "F+"},
		},
		{
			name:              "35 Default Sharding, replicationFactor = 3, strong quorum, unavailability with quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user6",
			unavailableRulers: []string{"ruler3"},
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "36 Default Sharding, replicationFactor = 3, strong quorum, disagreement no quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user9",
			expectedError:     errUnableToObtainQuorum,
		},
		{
			name:              "37 Default Sharding, replicationFactor = 3, strong quorum, unavailability no quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user6",
			unavailableRulers: []string{"ruler3", "ruler4"},
			expectedError:     errUnableToObtainQuorum,
		},
		{
			name:              "38 Shuffle Sharding, ShardSize = 6, strong quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 1,
			quorum:            Strong,
			user:              "user11",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "39 Shuffle Sharding, ShardSize = 6, strong quorum, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 1,
			quorum:            Strong,
			user:              "user12",
			expectedGroups:    []string{"A", "B", "C"},
		},
		{
			name:              "40 Shuffle Sharding, ShardSize = 6, replicationFactor = 2, strong quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 2,
			quorum:            Strong,
			user:              "user13",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "41 Shuffle Sharding, ShardSize = 6, replicationFactor = 2, strong quorum, sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 2,
			quorum:            Strong,
			user:              "user14",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "42 Shuffle Sharding, ShardSize = 6, replicationFactor = 2, strong quorum, disagreement",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 2,
			quorum:            Strong,
			user:              "user15",
			expectedGroups:    []string{"A+", "B+", "C+", "D", "E", "F+"},
		},
		{
			name:              "43 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, strong quorum, non-sparse",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user16",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "44 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, strong quorum, split",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user17",
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "45 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, strong quorum, disagreement with quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user18",
			expectedGroups:    []string{"A+", "B+", "C", "D", "E", "F+"},
		},
		{
			name:              "46 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, strong quorum, unavailability with quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user16",
			unavailableRulers: []string{"ruler3"},
			expectedGroups:    []string{"A", "B", "C", "D", "E", "F"},
		},
		{
			name:              "47 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, strong quorum, disagreement no quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user19",
			expectedError:     errUnableToObtainQuorum,
		},
		{
			name:              "48 Shuffle Sharding, ShardSize = 6, replicationFactor = 3, strong quorum, unavailability no quorum",
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  6,
			replicationFactor: 3,
			quorum:            Strong,
			user:              "user16",
			unavailableRulers: []string{"ruler3", "ruler4"},
			expectedError:     errUnableToRetrieveRules,
		},
	}

	// sort test cases by configuration so that similar test configurations are grouped together
	sort.Slice(testCases, func(i int, j int) bool {
		return testCaseConfigLess(testCases[i], testCases[j])
	})

	allUsers := map[string]bool{}
	allRulesByRuler := map[string]rulespb.RuleGroupList{}
	rulerTokens := map[string][]uint32{}

	for rID := range expectedRules {
		rulerTokens[rID] = []uint32{getRulerToken(rID)}

		for user, rules := range expectedRules[rID] {
			allUsers[user] = true
			allRulesByRuler[rID] = append(allRulesByRuler[rID], rules...)
		}
	}

	t.Logf("Running tests")
	for tcIndex, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var rulerAddrMap map[string]*Ruler

			forEachRuler := func(f func(rID string, r *Ruler)) {
				for rID, r := range rulerAddrMap {
					f(rID, r)
				}
			}

			if tcIndex > 0 && testCaseConfigEqual(testCases[tcIndex-1], testCases[tcIndex]) {
				t.Logf("We can re-use the previous ruler configuration")
			} else {
				t.Logf("Configuring rulers")

				ringKVStore, ringKVCleanUp := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
				rulerAddrMap = map[string]*Ruler{}
				rulerAddrMapForClients := map[string]*Ruler{}

				haTrackerKVStore, haTrackerKVCleanUp := consul.NewInMemoryClient(ha.GetReplicaDescCodec(), log.NewNopLogger(), nil)

				t.Cleanup(func() {
					assert.NoError(t, ringKVCleanUp.Close())
					assert.NoError(t, haTrackerKVCleanUp.Close())
				})

				t.Logf("Creating rulers")
				for rID := range expectedRules {
					t.Logf("Creating ruler %s", rID)
					store := newMockRuleStore(expectedRules[rID])
					cfg := defaultRulerConfig(t)

					cfg.ShardingStrategy = tc.shardingStrategy
					cfg.EnableSharding = tc.sharding

					cfg.Ring = RingConfig{
						InstanceID:   rID,
						InstanceAddr: rID,
						KVStore: kv.Config{
							Mock: ringKVStore,
						},
						ReplicationFactor: tc.replicationFactor,
					}

					cfg.HATrackerConfig = HATrackerConfig{
						EnableHATracker:        true,
						UpdateTimeout:          60 * time.Second,
						UpdateTimeoutJitterMax: 0,
						FailoverTimeout:        300 * time.Second,
						KVStore: kv.Config{
							Mock: haTrackerKVStore,
						},
						ReplicaID: rID,
					}

					testRuleGroupHash := func(g *rulespb.RuleGroupDesc) uint32 {
						return binary.LittleEndian.Uint32(g.Options[0].Value)
					}
					r := buildRulerWithCustomGroupHash(t, cfg, nil, store, rulerAddrMapForClients, testRuleGroupHash)
					r.limits = ruleLimits{evalDelay: 0, tenantShard: tc.shuffleShardSize}

					rulerAddrMap[rID] = r
					if tc.unavailableRulers == nil || !sliceContains(t, rID, tc.unavailableRulers) {
						rulerAddrMapForClients[rID] = r
					}

					if r.ring != nil {
						require.NoError(t, services.StartAndAwaitRunning(context.Background(), r.ring))
						t.Cleanup(r.ring.StopAsync)
					}
				}

				t.Logf("Mapping ruler addresses")
				if tc.sharding {
					err := ringKVStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
						d, _ := in.(*ring.Desc)
						if d == nil {
							d = ring.NewDesc()
						}
						for rID, token := range rulerTokens {
							d.AddIngester(rID, rulerAddrMap[rID].lifecycler.GetInstanceAddr(), "", token, ring.ACTIVE, time.Now())
						}
						return d, true, nil
					})
					require.NoError(t, err)
					// Wait a bit to make sure ruler's ring is updated.
					time.Sleep(100 * time.Millisecond)
				}

				// Sync Rules
				forEachRuler(func(rID string, r *Ruler) {
					t.Logf("Syncing ruler %s", rID)
					r.syncRules(context.Background(), rulerSyncReasonInitial)
				})

				// sleep until rules are evaluated
				for i := 0; i < 40; i++ {
					t.Logf("%s:  Sleeping for 3 seconds", time.Now().String())
					time.Sleep(3 * time.Second)

					unevaluatedGroups := 0
					for rid, r := range rulerAddrMap {
						if rid == "ruler7" {
							continue
						}
						groups := r.manager.GetRules(tc.user)
						if len(groups) > 0 {
							for _, group := range groups {
								if group.GetLastEvaluation().IsZero() {
									unevaluatedGroups++
								}
							}
						}
					}
					if unevaluatedGroups == 0 {
						break
					}
				}

				// Stop evaluation to freeze ruler state
				forEachRuler(func(rID string, r *Ruler) {
					t.Logf("Stopping ruler %s", rID)
					r.manager.Stop()
				})
			}

			ctx := user.InjectOrgID(context.Background(), tc.user)
			forEachRuler(func(id string, r *Ruler) {
				// if rulerID is specified for this testcase and doesn't match, then skip
				if tc.rulerID != "" && tc.rulerID != id {
					return
				}

				mockPoolClient := r.clientsPool.(*mockRulerClientsPool)
				mockPoolClient.numberOfCalls.Store(0)

				t.Logf("Calling GetRules:  ruler=%s, u=%s", id, tc.user)
				rulegroups, err := r.GetRules(ctx, tc.quorum, RulesRequest{})

				if tc.expectedError == "" {
					require.NoError(t, err)

					encodedRuleGroups := make([]string, len(rulegroups))
					for i, rg := range rulegroups {
						encodedRg, err := encodeExpectedGroup(rg.Group)
						require.NoError(t, err)
						encodedRuleGroups[i] = encodedRg
					}
					sort.Strings(encodedRuleGroups)
					require.Equal(t, tc.expectedGroups, encodedRuleGroups)
				} else {
					require.Contains(t, err.Error(), tc.expectedError)
				}
			})
		})
	}
}

func TestSharding(t *testing.T) {
	const (
		user1 = "user1"
		user2 = "user2"
		user3 = "user3"
		zone1 = "zone1"
		zone2 = "zone2"
		zone3 = "zone3"
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
		sharding          bool
		shardingStrategy  string
		shuffleShardSize  int
		replicationFactor int
		zoneAwareness     bool
		setupRing         func(*ring.Desc)
		enabledUsers      []string
		disabledUsers     []string

		expectedRules expectedRulesMap
	}

	type rulerDesc struct {
		id   string
		host string
		port int
		addr string
		zone string
	}
	newRulerDesc := func(index int, zone string) rulerDesc {
		host := fmt.Sprintf("%d.%d.%d.%d", index, index, index, index)
		port := 9999
		return rulerDesc{
			id:   fmt.Sprintf("ruler-%d", index),
			host: host,
			port: port,
			addr: fmt.Sprintf("%s:%d", host, port),
			zone: zone,
		}
	}
	rulerDescs := [...]rulerDesc{
		newRulerDesc(1, zone1),
		newRulerDesc(2, zone2),
		newRulerDesc(3, zone3),
		newRulerDesc(4, zone1),
		newRulerDesc(5, zone2),
		newRulerDesc(6, zone3),
	}

	testCases := map[string]testCase{
		"0 no sharding": {
			sharding:      false,
			expectedRules: expectedRulesMap{rulerDescs[0].id: allRules},
		},

		"1 no sharding, single user allowed": {
			sharding:     false,
			enabledUsers: []string{user1},
			expectedRules: expectedRulesMap{rulerDescs[0].id: map[string]rulespb.RuleGroupList{
				user1: {user1Group1, user1Group2},
			}},
		},

		"2 no sharding, single user disabled": {
			sharding:      false,
			disabledUsers: []string{user1},
			expectedRules: expectedRulesMap{rulerDescs[0].id: map[string]rulespb.RuleGroupList{
				user2: {user2Group1},
				user3: {user3Group1},
			}},
		},

		"3 default sharding, single ruler": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{rulerDescs[0].id: allRules},
		},

		"4 default sharding, single ruler, single enabled user": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			enabledUsers:     []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{rulerDescs[0].id: map[string]rulespb.RuleGroupList{
				user1: {user1Group1, user1Group2},
			}},
		},

		"5 default sharding, single ruler, single disabled user": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			disabledUsers:    []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{rulerDescs[0].id: map[string]rulespb.RuleGroupList{
				user2: {user2Group1},
				user3: {user3Group1},
			}},
		},

		"6 default sharding, multiple ACTIVE rulerDescs": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
					user2: {user2Group1},
				},

				rulerDescs[1].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
					user3: {user3Group1},
				},
			},
		},

		"7 default sharding, multiple ACTIVE rulerDescs, single enabled user": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			enabledUsers:     []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},

				rulerDescs[1].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
			},
		},

		"8 default sharding, multiple ACTIVE rulerDescs, single disabled user": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			disabledUsers:    []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
				},

				rulerDescs[1].id: map[string]rulespb.RuleGroupList{
					user3: {user3Group1},
				},
			},
		},

		"9 default sharding, unhealthy ACTIVE ruler": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.Ingesters[rulerDescs[1].id] = ring.InstanceDesc{
					Addr:      rulerDescs[1].addr,
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}),
				}
			},

			expectedRules: expectedRulesMap{
				// This ruler doesn't get rules from unhealthy ruler (RF=1).
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
					user2: {user2Group1},
				},
				rulerDescs[1].id: noRules,
			},
		},

		"10 default sharding, LEAVING ruler": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.LEAVING, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				// LEAVING ruler doesn't get any rules.
				rulerDescs[0].id: noRules,
				rulerDescs[1].id: allRules,
			},
		},

		"11 default sharding, JOINING ruler": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.JOINING, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				// JOINING ruler has no rules yet.
				rulerDescs[0].id: noRules,
				rulerDescs[1].id: allRules,
			},
		},

		"12 shuffle sharding, single ruler": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{0}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: allRules,
			},
		},

		"13 shuffle sharding, multiple rulerDescs, shard size 1": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 1,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, "") + 1, userToken(user2, 0, "") + 1, userToken(user3, 0, "") + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{user1Group1Token + 1, user1Group2Token + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: allRules,
				rulerDescs[1].id: noRules,
			},
		},

		// Same test as previous one, but with shard size=2. Second ruler gets all the rules.
		"14 shuffle sharding, two rulerDescs, shard size 2": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,

			setupRing: func(desc *ring.Desc) {
				// Exact same tokens setup as previous test.
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, "") + 1, userToken(user2, 0, "") + 1, userToken(user3, 0, "") + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{user1Group1Token + 1, user1Group2Token + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: noRules,
				rulerDescs[1].id: allRules,
			},
		},

		"15 shuffle sharding, two rulerDescs, shard size 1, distributed users": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 1,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, "") + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{userToken(user2, 0, "") + 1, userToken(user3, 0, "") + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				rulerDescs[1].id: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},
		"16 shuffle sharding, three rulerDescs, shard size 2": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, "") + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{userToken(user1, 1, "") + 1, user1Group2Token + 1, userToken(user2, 1, "") + 1, userToken(user3, 1, "") + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[2].id, rulerDescs[2].addr, rulerDescs[2].zone, sortTokens([]uint32{userToken(user2, 0, "") + 1, userToken(user3, 0, "") + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},
				rulerDescs[1].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
				rulerDescs[2].id: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},
		"17 shuffle sharding, three rulerDescs, shard size 2, ruler2 has no users": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, "") + 1, userToken(user2, 1, "") + 1, user1Group1Token + 1, user1Group2Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{userToken(user1, 1, "") + 1, userToken(user3, 1, "") + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[2].id, rulerDescs[2].addr, rulerDescs[2].zone, sortTokens([]uint32{userToken(user2, 0, "") + 1, userToken(user3, 0, "") + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				rulerDescs[1].id: noRules, // Ruler2 owns token for user2group1, but user-2 will only be handled by ruler-1 and 3.
				rulerDescs[2].id: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},

		"18 shuffle sharding, three rulerDescs, shard size 2, single enabled user": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,
			enabledUsers:     []string{user1},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, "") + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{userToken(user1, 1, "") + 1, user1Group2Token + 1, userToken(user2, 1, "") + 1, userToken(user3, 1, "") + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[2].id, rulerDescs[2].addr, rulerDescs[2].zone, sortTokens([]uint32{userToken(user2, 0, "") + 1, userToken(user3, 0, "") + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},
				rulerDescs[1].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
				rulerDescs[2].id: map[string]rulespb.RuleGroupList{},
			},
		},

		"19 shuffle sharding, three rulerDescs, shard size 2, single disabled user": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,
			disabledUsers:    []string{user1},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, "") + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{userToken(user1, 1, "") + 1, user1Group2Token + 1, userToken(user2, 1, "") + 1, userToken(user3, 1, "") + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[2].id, rulerDescs[2].addr, rulerDescs[2].zone, sortTokens([]uint32{userToken(user2, 0, "") + 1, userToken(user3, 0, "") + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{},
				rulerDescs[1].id: map[string]rulespb.RuleGroupList{},
				rulerDescs[2].id: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},

		"20 shuffle sharding, three rulerDescs, shard size 3, zone awareness enabled, single disabled user": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 3,
			zoneAwareness:    true,
			disabledUsers:    []string{user3},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, zone1) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{userToken(user1, 1, zone2) + 1, user1Group2Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[2].id, rulerDescs[2].addr, rulerDescs[2].zone, sortTokens([]uint32{userToken(user2, 0, zone3) + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},
				rulerDescs[1].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
				rulerDescs[2].id: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
				},
			},
		},

		"21 shuffle sharding, three rulerDescs, shard size 3, replicationFactor 3, single enabled user": {
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  3,
			replicationFactor: 3,
			enabledUsers:      []string{user1},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, zone1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{userToken(user1, 1, zone2) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[2].id, rulerDescs[2].addr, rulerDescs[2].zone, sortTokens([]uint32{userToken(user1, 2, zone3) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[3].id, rulerDescs[3].addr, rulerDescs[3].zone, sortTokens([]uint32{userToken(user1, 3, zone1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[4].id, rulerDescs[4].addr, rulerDescs[4].zone, sortTokens([]uint32{userToken(user1, 4, zone2) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[5].id, rulerDescs[5].addr, rulerDescs[5].zone, sortTokens([]uint32{userToken(user1, 5, zone3) + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				rulerDescs[1].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				rulerDescs[2].id: map[string]rulespb.RuleGroupList{},
				rulerDescs[3].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				rulerDescs[4].id: map[string]rulespb.RuleGroupList{},
				rulerDescs[5].id: map[string]rulespb.RuleGroupList{},
			},
		},

		"22 shuffle sharding, three rulerDescs, shard size 3, replicationFactor 3, zone awareness enabled, single enabled user": {
			sharding:          true,
			shardingStrategy:  util.ShardingStrategyShuffle,
			shuffleShardSize:  3,
			replicationFactor: 3,
			zoneAwareness:     true,
			enabledUsers:      []string{user1},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(rulerDescs[0].id, rulerDescs[0].addr, rulerDescs[0].zone, sortTokens([]uint32{userToken(user1, 0, zone1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[1].id, rulerDescs[1].addr, rulerDescs[1].zone, sortTokens([]uint32{userToken(user1, 1, zone2) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[2].id, rulerDescs[2].addr, rulerDescs[2].zone, sortTokens([]uint32{userToken(user1, 2, zone3) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[3].id, rulerDescs[3].addr, rulerDescs[3].zone, sortTokens([]uint32{userToken(user1, 3, zone1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[4].id, rulerDescs[4].addr, rulerDescs[4].zone, sortTokens([]uint32{userToken(user1, 4, zone2) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(rulerDescs[5].id, rulerDescs[5].addr, rulerDescs[5].zone, sortTokens([]uint32{userToken(user1, 5, zone3) + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				rulerDescs[0].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				rulerDescs[1].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				rulerDescs[2].id: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				rulerDescs[3].id: map[string]rulespb.RuleGroupList{},
				rulerDescs[4].id: map[string]rulespb.RuleGroupList{},
				rulerDescs[5].id: map[string]rulespb.RuleGroupList{},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ringKvStore, ringKVCleanUp := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			haTrackerKVStore, haTrackerKVCleanUp := consul.NewInMemoryClient(ha.GetReplicaDescCodec(), log.NewNopLogger(), nil)

			t.Cleanup(func() {
				assert.NoError(t, ringKVCleanUp.Close())
				assert.NoError(t, haTrackerKVCleanUp.Close())
			})

			rulerReplicationFactor := tc.replicationFactor
			if rulerReplicationFactor == 0 {
				rulerReplicationFactor = 1
			}

			setupRuler := func(rulerInstance rulerDesc, forceRing *ring.Ring) *Ruler {
				store := newMockRuleStore(allRules)
				cfg := Config{
					EnableSharding:   tc.sharding,
					ShardingStrategy: tc.shardingStrategy,
					Ring: RingConfig{
						InstanceID:   rulerInstance.id,
						InstanceAddr: rulerInstance.host,
						InstancePort: rulerInstance.port,
						KVStore: kv.Config{
							Mock: ringKvStore,
						},
						HeartbeatTimeout:     1 * time.Minute,
						ReplicationFactor:    rulerReplicationFactor,
						ZoneAwarenessEnabled: tc.zoneAwareness,
						InstanceZone:         rulerInstance.zone,
					},
					FlushCheckPeriod: 0,
					EnabledTenants:   tc.enabledUsers,
					DisabledTenants:  tc.disabledUsers,
					HATrackerConfig: HATrackerConfig{
						EnableHATracker:        true,
						UpdateTimeout:          60 * time.Second,
						UpdateTimeoutJitterMax: 0,
						FailoverTimeout:        300 * time.Second,
						KVStore: kv.Config{
							Mock: haTrackerKVStore,
						},
						ReplicaID: rulerInstance.id,
					},
				}

				r := buildRuler(t, cfg, nil, store, nil)
				r.limits = ruleLimits{evalDelay: 0, tenantShard: tc.shuffleShardSize}

				if forceRing != nil {
					r.ring = forceRing
				}
				return r
			}

			rulers := [len(rulerDescs)]*Ruler{}
			rulers[0] = setupRuler(rulerDescs[0], nil)

			rulerRing := rulers[0].ring

			// We start ruler's ring, but nothing else (not even lifecycler).
			if rulerRing != nil {
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), rulerRing))
				t.Cleanup(rulerRing.StopAsync)
			}

			if rulerRing != nil {
				// Reuse ring from r1.
				for i := 1; i < len(rulerDescs); i++ {
					rulers[i] = setupRuler(rulerDescs[i], rulerRing)
				}
			}

			if tc.setupRing != nil {
				err := ringKvStore.CAS(context.Background(), ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
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

			// Always add ruler1 to expected rulerDescs, even if there is no ring (no sharding).
			loadedRules1, err := rulers[0].listRules(context.Background())
			require.NoError(t, err)

			expected := expectedRulesMap{
				rulerDescs[0].id: loadedRules1,
			}

			addToExpected := func(id string, r *Ruler) {
				// Only expect rules from other rulerDescs when using ring, and they are present in the ring.
				if r != nil && rulerRing != nil && rulerRing.HasInstance(id) {
					loaded, err := r.listRules(context.Background())
					require.NoError(t, err)
					// Normalize nil map to empty one.
					if loaded == nil {
						loaded = map[string]rulespb.RuleGroupList{}
					}
					expected[id] = loaded
				}
			}

			for i := 1; i < len(rulerDescs); i++ {
				addToExpected(rulerDescs[i].id, rulers[i])
			}

			require.Equal(t, tc.expectedRules, expected)
		})
	}
}

// User shuffle shard token.
func userToken(user string, skip int, zone string) uint32 {
	r := rand.New(rand.NewSource(util.ShuffleShardSeed(user, zone)))

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
	store := newMockRuleStore(mockRules)
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
	store := newMockRuleStore(mockRules)
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
	d.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		model.Matrix{
			&model.SampleStream{
				Metric: model.Metric{
					labels.MetricName: "ALERTS_FOR_STATE",
					// user1's only alert rule
					labels.AlertName: model.LabelValue(mockRules["user1"][0].GetRules()[0].Alert),
				},
				Values: []model.SamplePair{{Timestamp: model.Time(downAtTimeMs), Value: model.SampleValue(downAtActiveSec)}},
			},
		},
		nil)
	d.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Panic("This should not be called for the ruler use-cases.")
	querierConfig := querier.DefaultQuerierConfig()
	querierConfig.IngesterStreaming = false

	// set up an empty store
	queryables := []querier.QueryableWithFilter{
		querier.UseAlwaysQueryable(newEmptyQueryable()),
	}

	// create a ruler but don't start it. instead, we'll evaluate the rule groups manually.
	r := buildRuler(t, rulerCfg, &querier.TestConfig{Cfg: querierConfig, Distributor: d, Stores: queryables}, store, nil)
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
