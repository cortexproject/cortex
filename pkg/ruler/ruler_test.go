package ruler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/prometheus/prometheus/notifier"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/user"
)

type mockRuleStore struct {
	rules map[string]rulefmt.RuleGroup

	pollPayload map[string][]configs.RuleGroup
}

func (m *mockRuleStore) PollRules(ctx context.Context) (map[string][]configs.RuleGroup, error) {
	pollPayload := m.pollPayload
	m.pollPayload = map[string][]configs.RuleGroup{}
	return pollPayload, nil
}

func (m *mockRuleStore) ListRuleGroups(ctx context.Context, options configs.RuleStoreConditions) (map[string]configs.RuleNamespace, error) {
	groupPrefix := userID + ":"

	namespaces := []string{}
	nss := map[string]configs.RuleNamespace{}
	for n := range m.rules {
		if strings.HasPrefix(n, groupPrefix) {
			components := strings.Split(n, ":")
			if len(components) != 3 {
				continue
			}
			namespaces = append(namespaces, components[1])
		}
	}

	if len(namespaces) == 0 {
		return nss, configs.ErrUserNotFound
	}

	for _, n := range namespaces {
		ns, err := m.getRuleNamespace(ctx, userID, n)
		if err != nil {
			continue
		}

		nss[n] = ns
	}

	return nss, nil
}

func (m *mockRuleStore) getRuleNamespace(ctx context.Context, userID string, namespace string) (configs.RuleNamespace, error) {
	groupPrefix := userID + ":" + namespace + ":"

	ns := configs.RuleNamespace{
		Groups: []rulefmt.RuleGroup{},
	}
	for n, g := range m.rules {
		if strings.HasPrefix(n, groupPrefix) {
			ns.Groups = append(ns.Groups, g)
		}
	}

	if len(ns.Groups) == 0 {
		return ns, configs.ErrGroupNamespaceNotFound
	}

	return ns, nil
}

func (m *mockRuleStore) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (rulefmt.RuleGroup, error) {
	groupID := userID + ":" + namespace + ":" + group
	g, ok := m.rules[groupID]

	if !ok {
		return rulefmt.RuleGroup{}, configs.ErrGroupNotFound
	}

	return g, nil

}

func (m *mockRuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group rulefmt.RuleGroup) error {
	groupID := userID + ":" + namespace + ":" + group.Name
	m.rules[groupID] = group
	return nil
}

func (m *mockRuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	groupID := userID + ":" + namespace + ":" + group
	delete(m.rules, groupID)
	return nil
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
