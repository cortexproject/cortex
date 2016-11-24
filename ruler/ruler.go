package ruler

import (
	"fmt"
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
	delay      time.Duration
	userID     string
	configsAPI configsAPI
	ruler      Ruler

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
		delay:      delay,
		userID:     cfg.UserID,
		configsAPI: configsAPI{configsAPIURL},
		ruler:      ruler,
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
	cfg, err := w.configsAPI.getOrgConfig(w.userID)
	if err != nil {
		return nil, fmt.Errorf("Error fetching config: %v", err)
	}
	rs, err := cfg.GetRules()
	if err != nil {
		return nil, fmt.Errorf("Error parsing rules: %v", err)
	}
	return rs, nil
}

func (w *worker) Stop() {
	close(w.done)
	<-w.terminated
}
