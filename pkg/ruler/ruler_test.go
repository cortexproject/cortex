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
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/user"
)

func defaultRulerConfig() Config {
	codec := codec.Proto{Factory: ring.ProtoDescFactory}
	consul := consul.NewInMemoryClient(codec)
	cfg := Config{
		StoreConfig: RuleStoreConfig{
			mock: newMockRuleStore(),
		},
	}
	flagext.DefaultValues(&cfg)
	flagext.DefaultValues(&cfg.Ring)
	cfg.Ring.KVStore.Mock = consul
	cfg.Ring.NumTokens = 1
	cfg.Ring.ListenPort = 0
	cfg.Ring.InstanceAddr = "localhost"
	cfg.Ring.InstanceID = "localhost"
	return cfg
}

func newTestRuler(t *testing.T, cfg Config) *Ruler {
	// TODO: Populate distributor and chunk store arguments to enable
	// other kinds of tests.

	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:    1e6,
		MaxConcurrent: 20,
		Timeout:       2 * time.Minute,
	})

	noopQueryable := storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return storage.NoopQuerier(), nil
	})

	l := log.NewLogfmtLogger(os.Stdout)
	l = level.NewFilter(l, level.AllowInfo())
	ruler, err := NewRuler(cfg, engine, noopQueryable, nil, prometheus.NewRegistry(), l)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure all rules are loaded before usage
	ruler.loadRules(context.Background())

	return ruler
}

func TestNotifierSendsUserIDHeader(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1) // We want one request to our test HTTP server.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, _, err := user.ExtractOrgIDFromHTTPRequest(r)
		assert.NoError(t, err)
		assert.Equal(t, userID, "1")
		wg.Done()
	}))
	defer ts.Close()

	cfg := defaultRulerConfig()
	err := cfg.AlertmanagerURL.Set(ts.URL)
	require.NoError(t, err)
	cfg.AlertmanagerDiscovery = false

	r := newTestRuler(t, cfg)
	defer r.Stop()
	n, err := r.getOrCreateNotifier("1")
	if err != nil {
		t.Fatal(err)
	}
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
	dir, err := ioutil.TempDir("/tmp", "ruler-tests")
	defer os.RemoveAll(dir)

	require.NoError(t, err)

	cfg := defaultRulerConfig()
	cfg.RulePath = dir

	r := newTestRuler(t, cfg)
	defer r.Stop()

	// test user1
	ctx := user.InjectOrgID(context.Background(), "user1")
	rls, err := r.Rules(ctx, &RulesRequest{})
	require.NoError(t, err)
	require.Len(t, rls.Groups, 1)
	rg := rls.Groups[0]
	expectedRg := mockRules["user1"][0]
	compareRuleGroupDescs(t, rg, expectedRg)

	// test user2
	ctx = user.InjectOrgID(context.Background(), "user2")
	rls, err = r.Rules(ctx, &RulesRequest{})
	require.NoError(t, err)
	require.Len(t, rls.Groups, 1)
	rg = rls.Groups[0]
	expectedRg = mockRules["user2"][0]
	compareRuleGroupDescs(t, rg, expectedRg)
}

func compareRuleGroupDescs(t *testing.T, expected, got *rules.RuleGroupDesc) {
	require.Equal(t, expected.Name, got.Name)
	require.Equal(t, expected.Namespace, got.Namespace)
	require.Len(t, got.Rules, len(expected.Rules))
	for i := range got.Rules {
		require.Equal(t, expected.Rules[i].Record, got.Rules[i].Record)
		require.Equal(t, expected.Rules[i].Alert, got.Rules[i].Alert)
	}
}
