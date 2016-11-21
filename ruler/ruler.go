package ruler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/querier"
	"github.com/weaveworks/cortex/user"
)

// Config is the configuration for the recording rules server.
type Config struct {
	DistributorConfig distributor.Config
	ConfigsAPIURL     string
	ExternalURL       string
	// How frequently to evaluate rules by default.
	EvaluationInterval time.Duration
	// XXX: Currently single tenant only (which is awful) as the most
	// expedient way of getting *something* working.
	UserID string
}

// Ruler is a recording rules server.
type Ruler struct {
	cfg         Config
	chunkStore  chunk.Store
	distributor *distributor.Distributor

	configsAPIURL *url.URL
	externalURL   *url.URL
}

// Worker does a thing until it's told to stop.
type Worker interface {
	Run()
	Stop()
}

type worker struct {
	delay         time.Duration
	userID        string
	configsAPIURL *url.URL
	opts          *rules.ManagerOptions
}

func (w *worker) Run() {
	var rs []rules.Rule
	for {
		var err error
		rs, err = w.loadRules()
		if err != nil {
			log.Warnf("Could not get configuration for %v: %v", w.userID, err)
			time.Sleep(w.delay)
		}
		break
	}
	group := rules.NewGroup("default", w.delay, rs, w.opts)
	for {
		// XXX: Use NewTicker.
		group.Eval()
		time.Sleep(w.delay)
	}
}

func (w *worker) loadRules() ([]rules.Rule, error) {
	cfg, err := getOrgConfig(w.configsAPIURL, w.userID)
	if err != nil {
		return nil, fmt.Errorf("Error fetching config: %v", err)
	}
	rs, err := loadRules(cfg.RulesFiles)
	if err != nil {
		return nil, fmt.Errorf("Error parsing rules: %v", err)
	}
	return rs, nil
}

func (w *worker) Stop() {
}

// New returns a new Ruler.
func New(chunkStore chunk.Store, cfg Config) (*Ruler, error) {
	configsAPIURL, err := url.Parse(cfg.ConfigsAPIURL)
	if err != nil {
		return nil, err
	}
	externalURL, err := url.Parse(cfg.ExternalURL)
	if err != nil {
		return nil, err
	}

	d, err := distributor.New(cfg.DistributorConfig)
	if err != nil {
		return nil, err
	}
	return &Ruler{
		cfg:           cfg,
		chunkStore:    chunkStore,
		distributor:   d,
		configsAPIURL: configsAPIURL,
		externalURL:   externalURL,
	}, nil
}

// GetWorkerFor gets a rules recording worker for the given user.
// It will keep polling until it can construct one.
func (r *Ruler) GetWorkerFor(userID string) Worker {
	delay := time.Duration(r.cfg.EvaluationInterval)
	return &worker{
		delay:         delay,
		userID:        userID,
		configsAPIURL: r.configsAPIURL,
		opts:          r.getManagerOptions(userID),
	}
}

func (r *Ruler) getManagerOptions(userID string) *rules.ManagerOptions {
	ctx := user.WithID(context.Background(), userID)
	appender := appenderAdapter{appender: r.distributor, ctx: ctx}
	queryable := querier.NewQueryable(r.distributor, r.chunkStore)
	engine := promql.NewEngine(queryable, nil)
	return &rules.ManagerOptions{
		SampleAppender: appender,
		Notifier:       nil,
		QueryEngine:    engine,
		Context:        ctx,
		ExternalURL:    r.externalURL,
	}
}

// loadRules loads rules.
//
// Strongly inspired by `loadGroups` in Prometheus.
func loadRules(files map[string]string) ([]rules.Rule, error) {
	result := []rules.Rule{}
	for fn, content := range files {
		stmts, err := promql.ParseStmts(string(content))
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %s", fn, err)
		}

		for _, stmt := range stmts {
			var rule rules.Rule

			switch r := stmt.(type) {
			case *promql.AlertStmt:
				rule = rules.NewAlertingRule(r.Name, r.Expr, r.Duration, r.Labels, r.Annotations)

			case *promql.RecordStmt:
				rule = rules.NewRecordingRule(r.Name, r.Expr, r.Labels)

			default:
				panic("ruler.loadRules: unknown statement type")
			}
			result = append(result, rule)
		}
	}
	return result, nil
}

// appenderAdapter adapts cortex.SampleAppender to prometheus.SampleAppender
type appenderAdapter struct {
	appender cortex.SampleAppender
	ctx      context.Context
}

func (a appenderAdapter) Append(sample *model.Sample) error {
	return a.appender.Append(a.ctx, []*model.Sample{sample})
}

func (a appenderAdapter) NeedsThrottling() bool {
	// XXX: Just a guess. Who knows?
	return false
}

type cortexConfig struct {
	RulesFiles map[string]string `json:"rules_files"`
}

// getOrgConfig gets the organization's cortex config from a configs api server.
func getOrgConfig(configsAPIURL *url.URL, userID string) (*cortexConfig, error) {
	// TODO: Extract configs client logic into go client library (ala users)
	// TODO: Fix configs server so that we not need org ID in the URL to get authenticated org
	url := fmt.Sprintf("%s/api/configs/org/%s/cortex", configsAPIURL.String(), userID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-Scope-OrgID", userID)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Invalid response from configs server: %v", res.StatusCode)
	}
	var config cortexConfig
	if err := json.NewDecoder(res.Body).Decode(&config); err != nil {
		return nil, err
	}
	return &config, nil
}
