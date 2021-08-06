package ruler

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore/objectclient"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func defaultRulerConfig(store rulestore.RuleStore) (Config, func()) {
	// Create a new temporary directory for the rules, so that
	// each test will run in isolation.
	rulesDir, _ := ioutil.TempDir("/tmp", "ruler-tests")
	codec := ring.GetCodec()
	consul := consul.NewInMemoryClient(codec)
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.RulePath = rulesDir
	cfg.StoreConfig.mock = store
	cfg.Ring.KVStore.Mock = consul
	cfg.Ring.NumTokens = 1
	cfg.Ring.ListenPort = 0
	cfg.Ring.InstanceAddr = "localhost"
	cfg.Ring.InstanceID = "localhost"
	cfg.EnableQueryStats = false

	// Create a cleanup function that will be called at the end of the test
	cleanup := func() {
		defer os.RemoveAll(rulesDir)
	}

	return cfg, cleanup
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

func testSetup(t *testing.T, cfg Config) (*promql.Engine, storage.QueryableFunc, Pusher, log.Logger, RulesLimits, func()) {
	dir, err := ioutil.TempDir("", filepath.Base(t.Name()))
	assert.NoError(t, err)
	cleanup := func() {
		os.RemoveAll(dir)
	}

	tracker := promql.NewActiveQueryTracker(dir, 20, log.NewNopLogger())

	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:         1e6,
		ActiveQueryTracker: tracker,
		Timeout:            2 * time.Minute,
	})

	noopQueryable := storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return storage.NoopQuerier(), nil
	})

	// Mock the pusher
	pusher := newPusherMock()
	pusher.MockPush(&cortexpb.WriteResponse{}, nil)

	l := log.NewLogfmtLogger(os.Stdout)
	l = level.NewFilter(l, level.AllowInfo())

	return engine, noopQueryable, pusher, l, ruleLimits{evalDelay: 0, maxRuleGroups: 20, maxRulesPerRuleGroup: 15}, cleanup
}

func newManager(t *testing.T, cfg Config) (*DefaultMultiTenantManager, func()) {
	engine, noopQueryable, pusher, logger, overrides, cleanup := testSetup(t, cfg)
	manager, err := NewDefaultMultiTenantManager(cfg, DefaultTenantManagerFactory(cfg, pusher, noopQueryable, engine, overrides, nil), prometheus.NewRegistry(), logger)
	require.NoError(t, err)

	return manager, cleanup
}

func newRuler(t *testing.T, cfg Config) (*Ruler, func()) {
	engine, noopQueryable, pusher, logger, overrides, cleanup := testSetup(t, cfg)
	storage, err := NewLegacyRuleStore(cfg.StoreConfig, promRules.FileLoader{}, log.NewNopLogger())
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	managerFactory := DefaultTenantManagerFactory(cfg, pusher, noopQueryable, engine, overrides, reg)
	manager, err := NewDefaultMultiTenantManager(cfg, managerFactory, reg, log.NewNopLogger())
	require.NoError(t, err)

	ruler, err := NewRuler(
		cfg,
		manager,
		reg,
		logger,
		storage,
		overrides,
	)
	require.NoError(t, err)

	return ruler, cleanup
}

func newTestRuler(t *testing.T, cfg Config) (*Ruler, func()) {
	ruler, cleanup := newRuler(t, cfg)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ruler))

	// Ensure all rules are loaded before usage
	ruler.syncRules(context.Background(), rulerSyncReasonInitial)

	return ruler, cleanup
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

	// We create an empty rule store so that the ruler will not load any rule from it.
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(nil))
	defer cleanup()

	cfg.AlertmanagerURL = ts.URL
	cfg.AlertmanagerDiscovery = false

	manager, rcleanup := newManager(t, cfg)
	defer rcleanup()
	defer manager.Stop()

	n, err := manager.getOrCreateNotifier("1")
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
		# HELP cortex_prometheus_notifications_dropped_total Total number of alerts dropped due to errors when sending to Alertmanager.
		# TYPE cortex_prometheus_notifications_dropped_total counter
		cortex_prometheus_notifications_dropped_total{user="1"} 0
	`), "cortex_prometheus_notifications_dropped_total"))
}

func TestRuler_Rules(t *testing.T) {
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(mockRules))
	defer cleanup()

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
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
		sharding         bool
		shardingStrategy string
		shuffleShardSize int
		setupRing        func(*ring.Desc)
		enabledUsers     []string
		disabledUsers    []string

		expectedRules expectedRulesMap
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
			sharding:      false,
			expectedRules: expectedRulesMap{ruler1: allRules},
		},

		"no sharding, single user allowed": {
			sharding:     false,
			enabledUsers: []string{user1},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user1: {user1Group1, user1Group2},
			}},
		},

		"no sharding, single user disabled": {
			sharding:      false,
			disabledUsers: []string{user1},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user2: {user2Group1},
				user3: {user3Group1},
			}},
		},

		"default sharding, single ruler": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{ruler1: allRules},
		},

		"default sharding, single ruler, single enabled user": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			enabledUsers:     []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user1: {user1Group1, user1Group2},
			}},
		},

		"default sharding, single ruler, single disabled user": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			disabledUsers:    []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user2: {user2Group1},
				user3: {user3Group1},
			}},
		},

		"default sharding, multiple ACTIVE rulers": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			enabledUsers:     []string{user1},
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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,
			disabledUsers:    []string{user1},
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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,

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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,

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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyDefault,

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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{0}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: allRules,
			},
		},

		"shuffle sharding, multiple rulers, shard size 1": {
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 1,

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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,

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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 1,

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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,

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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,

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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,
			enabledUsers:     []string{user1},

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
			sharding:         true,
			shardingStrategy: util.ShardingStrategyShuffle,
			shuffleShardSize: 2,
			disabledUsers:    []string{user1},

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
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			kvStore := consul.NewInMemoryClient(ring.GetCodec())

			setupRuler := func(id string, host string, port int, forceRing *ring.Ring) *Ruler {
				cfg := Config{
					StoreConfig:      RuleStoreConfig{mock: newMockRuleStore(allRules)},
					EnableSharding:   tc.sharding,
					ShardingStrategy: tc.shardingStrategy,
					Ring: RingConfig{
						InstanceID:   id,
						InstanceAddr: host,
						InstancePort: port,
						KVStore: kv.Config{
							Mock: kvStore,
						},
						HeartbeatTimeout: 1 * time.Minute,
					},
					FlushCheckPeriod: 0,
					EnabledTenants:   tc.enabledUsers,
					DisabledTenants:  tc.disabledUsers,
				}

				r, cleanup := newRuler(t, cfg)
				r.limits = ruleLimits{evalDelay: 0, tenantShard: tc.shuffleShardSize}
				t.Cleanup(cleanup)

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
				err := kvStore.CAS(context.Background(), ring.RulerRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
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
			loadedRules1, err := r1.listRules(context.Background())
			require.NoError(t, err)

			expected := expectedRulesMap{
				ruler1: loadedRules1,
			}

			addToExpected := func(id string, r *Ruler) {
				// Only expect rules from other rulers when using ring, and they are present in the ring.
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

			addToExpected(ruler2, r2)
			addToExpected(ruler3, r3)

			require.Equal(t, tc.expectedRules, expected)
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

func TestDeleteTenantRuleGroups(t *testing.T) {
	ruleGroups := []ruleGroupKey{
		{user: "userA", namespace: "namespace", group: "group"},
		{user: "userB", namespace: "namespace1", group: "group"},
		{user: "userB", namespace: "namespace2", group: "group"},
	}

	obj, rs := setupRuleGroupsStore(t, ruleGroups)
	require.Equal(t, 3, obj.GetObjectCount())

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
		require.Equal(t, 3, obj.GetObjectCount())

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", false)
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	{
		callDeleteTenantAPI(t, api, "userA")
		require.Equal(t, 2, obj.GetObjectCount())

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Just deleted.
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	// Deleting same user again works fine and reports no problems.
	{
		callDeleteTenantAPI(t, api, "userA")
		require.Equal(t, 2, obj.GetObjectCount())

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Already deleted before.
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	{
		callDeleteTenantAPI(t, api, "userB")
		require.Equal(t, 0, obj.GetObjectCount())

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Deleted previously
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", true)                    // Just deleted
	}
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

func setupRuleGroupsStore(t *testing.T, ruleGroups []ruleGroupKey) (*chunk.MockStorage, rulestore.RuleStore) {
	obj := chunk.NewMockStorage()
	rs := objectclient.NewRuleStore(obj, 5, log.NewNopLogger())

	// "upload" rule groups
	for _, key := range ruleGroups {
		desc := rulespb.ToProto(key.user, key.namespace, rulefmt.RuleGroup{Name: key.group})
		require.NoError(t, rs.SetRuleGroup(context.Background(), key.user, key.namespace, desc))
	}

	return obj, rs
}

type ruleGroupKey struct {
	user, namespace, group string
}

func TestRuler_ListAllRules(t *testing.T) {
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(mockRules))
	defer cleanup()

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	router := mux.NewRouter()
	router.Path("/ruler/rule_groups").Methods(http.MethodGet).HandlerFunc(r.ListAllRules)

	req := requestFor(t, http.MethodGet, "https://localhost:8080/ruler/rule_groups", nil, "")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	// Check status code and header
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/yaml", resp.Header.Get("Content-Type"))

	gs := make(map[string]map[string][]rulefmt.RuleGroup) // user:namespace:[]rulefmt.RuleGroup
	for userID := range mockRules {
		gs[userID] = mockRules[userID].Formatted()
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
