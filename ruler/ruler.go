package ruler

import (
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/chunk"
	configs "github.com/weaveworks/cortex/configs/client"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/querier"
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

	// URL of the Alertmanager to send notifications to.
	AlertmanagerURL util.URLValue
	// Whether to use DNS SRV records to discover alertmanagers.
	AlertmanagerDiscovery bool
	// How long to wait between refreshing the list of alertmanagers based on
	// DNS service discovery.
	AlertmanagerRefreshInterval time.Duration

	// Capacity of the queue for notifications to be sent to the Alertmanager.
	NotificationQueueCapacity int
	// HTTP timeout duration when sending notifications to the Alertmanager.
	NotificationTimeout time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.ExternalURL.URL, _ = url.Parse("") // Must be non-nil
	f.Var(&cfg.ConfigsAPIURL, "ruler.configs.url", "URL of configs API server.")
	f.Var(&cfg.ExternalURL, "ruler.external.url", "URL of alerts return path.")
	f.DurationVar(&cfg.EvaluationInterval, "ruler.evaluation-interval", 15*time.Second, "How frequently to evaluate rules")
	f.DurationVar(&cfg.ClientTimeout, "ruler.client-timeout", 5*time.Second, "Timeout for requests to Weave Cloud configs service.")
	f.IntVar(&cfg.NumWorkers, "ruler.num-workers", 1, "Number of rule evaluator worker routines in this process")
	f.Var(&cfg.AlertmanagerURL, "ruler.alertmanager-url", "URL of the Alertmanager to send notifications to.")
	f.BoolVar(&cfg.AlertmanagerDiscovery, "ruler.alertmanager-discovery", false, "Use DNS SRV records to discover alertmanager hosts.")
	f.DurationVar(&cfg.AlertmanagerRefreshInterval, "ruler.alertmanager-refresh-interval", 1*time.Minute, "How long to wait between refreshing alertmanager hosts.")
	f.IntVar(&cfg.NotificationQueueCapacity, "ruler.notification-queue-capacity", 10000, "Capacity of the queue for notifications to be sent to the Alertmanager.")
	f.DurationVar(&cfg.NotificationTimeout, "ruler.notification-timeout", 10*time.Second, "HTTP timeout duration when sending notifications to the Alertmanager.")
}

// Ruler evaluates rules.
type Ruler struct {
	engine        *promql.Engine
	pusher        Pusher
	alertURL      *url.URL
	notifierCfg   *config.Config
	queueCapacity int

	// Per-user notifiers with separate queues.
	notifiersMtx sync.Mutex
	notifiers    map[string]*notifier.Notifier
}

// NewRuler creates a new ruler from a distributor and chunk store.
func NewRuler(cfg Config, d *distributor.Distributor, c *chunk.Store) (*Ruler, error) {
	ncfg, err := buildNotifierConfig(&cfg)
	if err != nil {
		return nil, err
	}
	return &Ruler{
		engine:        querier.NewEngine(d, c),
		pusher:        d,
		alertURL:      cfg.ExternalURL.URL,
		notifierCfg:   ncfg,
		queueCapacity: cfg.NotificationQueueCapacity,
		notifiers:     map[string]*notifier.Notifier{},
	}, nil
}

// Builds a Prometheus config.Config from a ruler.Config with just the required
// options to configure notifications to Alertmanager.
func buildNotifierConfig(rulerConfig *Config) (*config.Config, error) {
	if rulerConfig.AlertmanagerURL.URL == nil {
		return &config.Config{}, nil
	}

	u := rulerConfig.AlertmanagerURL
	var sdConfig config.ServiceDiscoveryConfig
	if rulerConfig.AlertmanagerDiscovery {
		dnsSDConfig := config.DNSSDConfig{
			Names:           []string{u.Host},
			RefreshInterval: model.Duration(rulerConfig.AlertmanagerRefreshInterval),
			Type:            "SRV",
			Port:            0, // Ignored, because of SRV.
		}
		sdConfig = config.ServiceDiscoveryConfig{
			DNSSDConfigs: []*config.DNSSDConfig{&dnsSDConfig},
		}
	} else {
		sdConfig = config.ServiceDiscoveryConfig{
			StaticConfigs: []*config.TargetGroup{
				{
					Targets: []model.LabelSet{
						{
							model.AddressLabel: model.LabelValue(u.Host),
						},
					},
				},
			},
		}
	}
	amConfig := &config.AlertmanagerConfig{
		Scheme:                 u.Scheme,
		PathPrefix:             u.Path,
		Timeout:                rulerConfig.NotificationTimeout,
		ServiceDiscoveryConfig: sdConfig,
	}

	promConfig := &config.Config{
		AlertingConfig: config.AlertingConfig{
			AlertmanagerConfigs: []*config.AlertmanagerConfig{amConfig},
		},
	}

	if u.User != nil {
		amConfig.HTTPClientConfig = config.HTTPClientConfig{
			BasicAuth: &config.BasicAuth{
				Username: u.User.Username(),
			},
		}

		if password, isSet := u.User.Password(); isSet {
			amConfig.HTTPClientConfig.BasicAuth.Password = password
		}
	}

	return promConfig, nil
}

func (r *Ruler) newGroup(ctx context.Context, rs []rules.Rule) (*rules.Group, error) {
	appender := appenderAdapter{pusher: r.pusher, ctx: ctx}
	userID, err := user.Extract(ctx)
	if err != nil {
		return nil, err
	}
	notifier, err := r.getOrCreateNotifier(userID)
	if err != nil {
		return nil, err
	}
	opts := &rules.ManagerOptions{
		SampleAppender: appender,
		QueryEngine:    r.engine,
		Context:        ctx,
		ExternalURL:    r.alertURL,
		Notifier:       notifier,
	}
	delay := 0 * time.Second // Unused, so 0 value is fine.
	return rules.NewGroup("default", delay, rs, opts), nil
}

func (r *Ruler) getOrCreateNotifier(userID string) (*notifier.Notifier, error) {
	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	n, ok := r.notifiers[userID]
	if ok {
		return n, nil
	}

	n = notifier.New(&notifier.Options{
		QueueCapacity: r.queueCapacity,
		Do: func(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
			// Note: The passed-in context comes from the Prometheus rule group code
			// and does *not* contain the userID. So it needs to be added to the context
			// here before using the context to inject the userID into the HTTP request.
			ctx = user.Inject(ctx, userID)
			if err := user.InjectIntoHTTPRequest(ctx, req); err != nil {
				return nil, err
			}
			return ctxhttp.Do(ctx, client, req)
		},
	})

	// This should never fail, unless there's a programming mistake.
	if err := n.ApplyConfig(r.notifierCfg); err != nil {
		return nil, err
	}
	go n.Run()

	// TODO: Remove notifiers for stale users. Right now this is a slow leak.
	r.notifiers[userID] = n
	return n, nil
}

// Evaluate a list of rules in the given context.
func (r *Ruler) Evaluate(ctx context.Context, rs []rules.Rule) {
	log.Debugf("Evaluating %d rules...", len(rs))
	start := time.Now()
	g, err := r.newGroup(ctx, rs)
	if err != nil {
		log.Errorf("Failed to create rule group: %v", err)
		return
	}
	g.Eval()

	// The prometheus routines we're calling have their own instrumentation
	// but, a) it's rule-based, not group-based, b) it's a summary, not a
	// histogram, so we can't reliably aggregate.
	evalDuration.Observe(time.Since(start).Seconds())
	rulesProcessed.Add(float64(len(rs)))
}

// Stop stops the Ruler.
func (r *Ruler) Stop() {
	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	for _, n := range r.notifiers {
		n.Stop()
	}
}

// Server is a rules server.
type Server struct {
	scheduler *scheduler
	workers   []worker
}

// NewServer makes a new rule processing server.
func NewServer(cfg Config, ruler *Ruler) (*Server, error) {
	c := configs.RulesAPI{
		URL:     cfg.ConfigsAPIURL.URL,
		Timeout: cfg.ClientTimeout,
	}
	// TODO: Separate configuration for polling interval.
	s := newScheduler(c, cfg.EvaluationInterval, cfg.EvaluationInterval)
	if cfg.NumWorkers <= 0 {
		return nil, fmt.Errorf("must have at least 1 worker, got %d", cfg.NumWorkers)
	}
	workers := make([]worker, cfg.NumWorkers)
	for i := 0; i < cfg.NumWorkers; i++ {
		workers[i] = newWorker(&s, ruler)
	}
	srv := Server{
		scheduler: &s,
		workers:   workers,
	}
	go srv.run()
	return &srv, nil
}

// Run the server.
func (s *Server) run() {
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
	ruler     *Ruler

	done       chan struct{}
	terminated chan struct{}
}

func newWorker(scheduler *scheduler, ruler *Ruler) worker {
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
		ctx := user.Inject(context.Background(), item.userID)
		w.ruler.Evaluate(ctx, item.rules)
		w.scheduler.workItemDone(*item)
		log.Debugf("%v handed back to queue", item)
	}
}

func (w *worker) Stop() {
	close(w.done)
	<-w.terminated
}
