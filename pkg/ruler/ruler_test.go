package ruler

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func defaultRulerConfig(store rules.RuleStore) (Config, func()) {
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

	// Create a cleanup function that will be called at the end of the test
	cleanup := func() {
		defer os.RemoveAll(rulesDir)
	}

	return cfg, cleanup
}

func newTestRuler(t *testing.T, cfg Config) (*Ruler, func()) {
	dir, err := ioutil.TempDir("", t.Name())
	testutil.Ok(t, err)
	cleanup := func() {
		os.RemoveAll(dir)
	}

	tracker := promql.NewActiveQueryTracker(dir, 20, util.Logger)

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
	pusher.MockPush(&client.WriteResponse{}, nil)

	l := log.NewLogfmtLogger(os.Stdout)
	l = level.NewFilter(l, level.AllowInfo())
	ruler, err := NewRuler(cfg, engine, noopQueryable, pusher, prometheus.NewRegistry(), l)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ruler))

	// Ensure all rules are loaded before usage
	ruler.loadRules(context.Background())

	return ruler, cleanup
}

func TestNotifierSendsUserIDHeader(t *testing.T) {
	var wg sync.WaitGroup

	// We do expect 1 API call for the user create with the getOrCreateNotifier()
	wg.Add(1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, _, err := user.ExtractOrgIDFromHTTPRequest(r)
		assert.NoError(t, err)
		assert.Equal(t, userID, "1")
		wg.Done()
	}))
	defer ts.Close()

	// We create an empty rule store so that the ruler will not load any rule from it.
	cfg, cleanup := defaultRulerConfig(newMockRuleStore(nil))
	defer cleanup()

	err := cfg.AlertmanagerURL.Set(ts.URL)
	require.NoError(t, err)
	cfg.AlertmanagerDiscovery = false

	r, rcleanup := newTestRuler(t, cfg)
	defer rcleanup()
	defer services.StopAndAwaitTerminated(context.Background(), r) //nolint:errcheck

	n, err := r.getOrCreateNotifier("1")
	require.NoError(t, err)

	for _, not := range r.notifiers {
		defer not.stop()
	}
	// Loop until notifier discovery syncs up
	for len(n.Alertmanagers()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	n.Send(&notifier.Alert{
		Labels: labels.Labels{labels.Label{Name: "alertname", Value: "testalert"}},
	})

	wg.Wait()
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

func compareRuleGroupDescToStateDesc(t *testing.T, expected *rules.RuleGroupDesc, got *GroupStateDesc) {
	require.Equal(t, got.Group.Name, expected.Name)
	require.Equal(t, got.Group.Namespace, expected.Namespace)
	require.Len(t, expected.Rules, len(got.ActiveRules))
	for i := range got.ActiveRules {
		require.Equal(t, expected.Rules[i].Record, got.ActiveRules[i].Rule.Record)
		require.Equal(t, expected.Rules[i].Alert, got.ActiveRules[i].Rule.Alert)
	}
}
