package ruler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/cortexproject/cortex/pkg/configs"
	client_config "github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/prometheus/prometheus/notifier"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/user"
)

type mockRuleStore struct{}

func (m *mockRuleStore) GetRules(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	return map[string]configs.VersionedRulesConfig{}, nil
}

func (m *mockRuleStore) GetAlerts(ctx context.Context, since configs.ID) (*client_config.ConfigsResponse, error) {
	return nil, nil
}

func defaultRulerConfig() Config {
	codec := codec.Proto{Factory: ring.ProtoDescFactory}
	consul := consul.NewInMemoryClient(codec)
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	flagext.DefaultValues(&cfg.LifecyclerConfig)
	cfg.LifecyclerConfig.RingConfig.ReplicationFactor = 1
	cfg.LifecyclerConfig.RingConfig.KVStore.Mock = consul
	cfg.LifecyclerConfig.NumTokens = 1
	cfg.LifecyclerConfig.FinalSleep = time.Duration(0)
	cfg.LifecyclerConfig.ListenPort = func(i int) *int { return &i }(0)
	cfg.LifecyclerConfig.Addr = "localhost"
	cfg.LifecyclerConfig.ID = "localhost"
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
	queryable := querier.NewQueryable(nil, nil, nil, 0)
	ruler, err := NewRuler(cfg, engine, queryable, nil, &mockRuleStore{})
	if err != nil {
		t.Fatal(err)
	}
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
	cfg.AlertmanagerURL.Set(ts.URL)
	cfg.AlertmanagerDiscovery = false

	r := newTestRuler(t, cfg)
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
