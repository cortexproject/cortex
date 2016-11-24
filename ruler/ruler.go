package ruler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/querier"
	"github.com/weaveworks/cortex/user"
)

// Config is the configuration for the recording rules server.
type Config struct {
	ConfigsAPIURL string
	ExternalURL   string
	// How frequently to evaluate rules by default.
	EvaluationInterval time.Duration
	// XXX: Currently single tenant only (which is awful) as the most
	// expedient way of getting *something* working.
	UserID string
}

// Ruler evaluates rules.
type Ruler struct {
	Engine   *promql.Engine
	Appender SampleAppender
}

// NewRuler creates a new ruler from a distributor and chunk store.
func NewRuler(d *distributor.Distributor, c chunk.Store) Ruler {
	return Ruler{querier.NewEngine(d, c), d}
}

func (r *Ruler) getManagerOptions(ctx context.Context) *rules.ManagerOptions {
	appender := appenderAdapter{appender: r.Appender, ctx: ctx}
	return &rules.ManagerOptions{
		SampleAppender: appender,
		QueryEngine:    r.Engine,
		Context:        ctx,
	}
}

func (r *Ruler) newGroup(ctx context.Context, delay time.Duration, rs []rules.Rule) *rules.Group {
	return rules.NewGroup("default", delay, rs, r.getManagerOptions(ctx))
}

// Evaluate a list of rules in the given context.
func (r *Ruler) Evaluate(ctx context.Context, rs []rules.Rule) {
	delay := 0 * time.Second // Unused, so 0 value is fine.
	g := r.newGroup(ctx, delay, rs)
	g.Eval()
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
	ruler         Ruler

	done       chan struct{}
	terminated chan struct{}
}

// NewWorker gets a rules recording worker for the given user.
// It will keep polling until it can construct one.
func NewWorker(cfg Config, ruler Ruler) (Worker, error) {
	configsAPIURL, err := url.Parse(cfg.ConfigsAPIURL)
	if err != nil {
		return nil, err
	}
	delay := time.Duration(cfg.EvaluationInterval)
	return &worker{
		delay:         delay,
		userID:        cfg.UserID,
		configsAPIURL: configsAPIURL,
		ruler:         ruler,
	}, nil
}

func (w *worker) Run() {
	defer close(w.terminated)
	rs := []rules.Rule{}
	ctx := user.WithID(context.Background(), w.userID)
	tick := time.NewTicker(w.delay)
	defer tick.Stop()
	for {
		var err error
		select {
		case <-w.done:
			return
		default:
		}
		// Select on 'done' again to avoid live-locking.
		select {
		case <-w.done:
			return
		case <-tick.C:
			if len(rs) == 0 {
				rs, err = w.loadRules()
				if err != nil {
					log.Warnf("Could not get configuration for %v: %v", w.userID, err)
					continue
				}
			} else {
				w.ruler.Evaluate(ctx, rs)
			}
		}
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
	close(w.done)
	<-w.terminated
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
