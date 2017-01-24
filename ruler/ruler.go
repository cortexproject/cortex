package ruler

import (
	"flag"
	"fmt"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/querier"
	"github.com/weaveworks/cortex/user"
	"github.com/weaveworks/cortex/util"
)

var (
	evalDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "group_evaluation_duration_seconds",
		Help:      "The duration for a rule group to execute.",
	})
	rulesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "rules_processed_total",
		Help:      "How many rules have been processed.",
	})
	blockedWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "blocked_workers",
		Help:      "How many workers are waiting on an item to be ready.",
	})
)

func init() {
	prometheus.MustRegister(evalDuration)
	prometheus.MustRegister(rulesProcessed)
	prometheus.MustRegister(blockedWorkers)
}

// Config is the configuration for the recording rules server.
type Config struct {
	ConfigsAPIURL util.URLValue

	// HTTP timeout duration for requests made to the Weave Cloud configs
	// service.
	ClientTimeout time.Duration

	// This is used for template expansion in alerts. Because we don't support
	// alerts yet, this value doesn't matter. However, it must be a valid URL
	// in order to navigate Prometheus's code paths.
	ExternalURL util.URLValue

	// How frequently to evaluate rules by default.
	EvaluationInterval time.Duration
	NumWorkers         int
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.Var(&cfg.ConfigsAPIURL, "ruler.configs.url", "URL of configs API server.")
	f.Var(&cfg.ExternalURL, "ruler.external.url", "URL of alerts return path.")
	f.DurationVar(&cfg.EvaluationInterval, "ruler.evaluation-interval", 15*time.Second, "How frequently to evaluate rules")
	f.DurationVar(&cfg.ClientTimeout, "ruler.client-timeout", 5*time.Second, "Timeout for requests to Weave Cloud configs service.")
	f.IntVar(&cfg.NumWorkers, "ruler.num-workers", 1, "Number of rule evaluator worker routines in this process")
}

// Ruler evaluates rules.
type Ruler struct {
	engine   *promql.Engine
	pusher   Pusher
	alertURL *url.URL
}

// NewRuler creates a new ruler from a distributor and chunk store.
func NewRuler(cfg Config, d *distributor.Distributor, c chunk.Store) Ruler {
	return Ruler{querier.NewEngine(d, c), d, cfg.ExternalURL.URL}
}

func (r *Ruler) newGroup(ctx context.Context, rs []rules.Rule) *rules.Group {
	appender := appenderAdapter{pusher: r.pusher, ctx: ctx}
	opts := &rules.ManagerOptions{
		SampleAppender: appender,
		QueryEngine:    r.engine,
		Context:        ctx,
		ExternalURL:    r.alertURL,
	}
	delay := 0 * time.Second // Unused, so 0 value is fine.
	return rules.NewGroup("default", delay, rs, opts)
}

// Evaluate a list of rules in the given context.
func (r *Ruler) Evaluate(ctx context.Context, rs []rules.Rule) {
	log.Debugf("Evaluating %d rules...", len(rs))
	start := time.Now()
	g := r.newGroup(ctx, rs)
	g.Eval()
	// The prometheus routines we're calling have their own instrumentation
	// but, a) it's rule-based, not group-based, b) it's a summary, not a
	// histogram, so we can't reliably aggregate.
	evalDuration.Observe(time.Since(start).Seconds())
	rulesProcessed.Add(float64(len(rs)))
}

// Server is a rules server.
type Server struct {
	scheduler *scheduler
	workers   []worker
}

// NewServer makes a new rule processing server.
func NewServer(cfg Config, ruler Ruler) (*Server, error) {
	c := configsAPI{cfg.ConfigsAPIURL.URL, cfg.ClientTimeout}
	// TODO: Separate configuration for polling interval.
	s := newScheduler(c, cfg.EvaluationInterval, cfg.EvaluationInterval)
	if cfg.NumWorkers <= 0 {
		return nil, fmt.Errorf("must have at least 1 worker, got %d", cfg.NumWorkers)
	}
	workers := make([]worker, cfg.NumWorkers)
	for i := 0; i < cfg.NumWorkers; i++ {
		workers[i] = newWorker(&s, ruler)
	}
	return &Server{
		scheduler: &s,
		workers:   workers,
	}, nil
}

// Run the server.
func (s *Server) Run() {
	go s.scheduler.Run()
	for _, w := range s.workers {
		go w.Run()
	}
	log.Infof("Ruler up and running")
}

// Stop the server.
func (s *Server) Stop() {
	for _, w := range s.workers {
		w.Stop()
	}
	s.scheduler.Stop()
}

// Worker does a thing until it's told to stop.
type Worker interface {
	Run()
	Stop()
}

type worker struct {
	scheduler *scheduler
	ruler     Ruler

	done       chan struct{}
	terminated chan struct{}
}

func newWorker(scheduler *scheduler, ruler Ruler) worker {
	return worker{
		scheduler:  scheduler,
		ruler:      ruler,
		done:       make(chan struct{}),
		terminated: make(chan struct{}),
	}
}

func (w *worker) Run() {
	defer close(w.terminated)
	for {
		select {
		case <-w.done:
			return
		default:
		}
		blockedWorkers.Inc()
		log.Debugf("Waiting for next work item")
		item := w.scheduler.nextWorkItem()
		blockedWorkers.Dec()
		if item == nil {
			log.Debugf("Queue closed and empty. Terminating worker.")
			return
		}
		log.Debugf("Processing %v", item)
		ctx := user.WithID(context.Background(), item.userID)
		w.ruler.Evaluate(ctx, item.rules)
		w.scheduler.workItemDone(*item)
		log.Debugf("%v handed back to queue", item)
	}
}

func (w *worker) Stop() {
	close(w.done)
	<-w.terminated
}
