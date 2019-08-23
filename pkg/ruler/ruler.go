package ruler

import (
	native_ctx "context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	ot "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/strutil"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/user"
)

var (
	evalDuration = instrument.NewHistogramCollectorFromOpts(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "group_evaluation_duration_seconds",
		Help:      "The duration for a rule group to execute.",
		Buckets:   []float64{.1, .25, .5, 1, 2.5, 5, 10, 25},
	})
	rulesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "rules_processed_total",
		Help:      "How many rules have been processed.",
	})
	ringCheckErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "ruler_ring_check_errors_total",
		Help:      "Number of errors that have occurred when checking the ring for ownership",
	})
	ruleMetrics *rules.Metrics
)

func init() {
	evalDuration.Register()
	ruleMetrics = rules.NewGroupMetrics(prometheus.DefaultRegisterer)
}

// Config is the configuration for the recording rules server.
type Config struct {
	// This is used for template expansion in alerts; must be a valid URL
	ExternalURL flagext.URLValue

	// How frequently to evaluate rules by default.
	EvaluationInterval time.Duration
	NumWorkers         int

	// URL of the Alertmanager to send notifications to.
	AlertmanagerURL flagext.URLValue
	// Whether to use DNS SRV records to discover alertmanagers.
	AlertmanagerDiscovery bool
	// How long to wait between refreshing the list of alertmanagers based on
	// DNS service discovery.
	AlertmanagerRefreshInterval time.Duration

	// Capacity of the queue for notifications to be sent to the Alertmanager.
	NotificationQueueCapacity int
	// HTTP timeout duration when sending notifications to the Alertmanager.
	NotificationTimeout time.Duration
	// Timeout for rule group evaluation, including sending result to ingester
	GroupTimeout time.Duration

	EnableSharding bool

	SearchPendingFor time.Duration
	LifecyclerConfig ring.LifecyclerConfig
	FlushCheckPeriod time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.LifecyclerConfig.RegisterFlagsWithPrefix("ruler.", f)

	cfg.ExternalURL.URL, _ = url.Parse("") // Must be non-nil
	f.Var(&cfg.ExternalURL, "ruler.external.url", "URL of alerts return path.")
	f.DurationVar(&cfg.EvaluationInterval, "ruler.evaluation-interval", 15*time.Second, "How frequently to evaluate rules")
	f.IntVar(&cfg.NumWorkers, "ruler.num-workers", 1, "Number of rule evaluator worker routines in this process")
	f.Var(&cfg.AlertmanagerURL, "ruler.alertmanager-url", "URL of the Alertmanager to send notifications to.")
	f.BoolVar(&cfg.AlertmanagerDiscovery, "ruler.alertmanager-discovery", false, "Use DNS SRV records to discover alertmanager hosts.")
	f.DurationVar(&cfg.AlertmanagerRefreshInterval, "ruler.alertmanager-refresh-interval", 1*time.Minute, "How long to wait between refreshing alertmanager hosts.")
	f.IntVar(&cfg.NotificationQueueCapacity, "ruler.notification-queue-capacity", 10000, "Capacity of the queue for notifications to be sent to the Alertmanager.")
	f.DurationVar(&cfg.NotificationTimeout, "ruler.notification-timeout", 10*time.Second, "HTTP timeout duration when sending notifications to the Alertmanager.")
	f.DurationVar(&cfg.GroupTimeout, "ruler.group-timeout", 10*time.Second, "Timeout for rule group evaluation, including sending result to ingester")
	if flag.Lookup("promql.lookback-delta") == nil {
		flag.DurationVar(&promql.LookbackDelta, "promql.lookback-delta", promql.LookbackDelta, "Time since the last sample after which a time series is considered stale and ignored by expression evaluations.")
	}
	f.DurationVar(&cfg.SearchPendingFor, "ruler.search-pending-for", 5*time.Minute, "Time to spend searching for a pending ruler when shutting down.")
	f.BoolVar(&cfg.EnableSharding, "ruler.enable-sharding", false, "Distribute rule evaluation using ring backend")
	f.DurationVar(&cfg.FlushCheckPeriod, "ruler.flush-period", 1*time.Minute, "Period with which to attempt to flush rule groups.")
}

// Ruler evaluates rules.
type Ruler struct {
	cfg         Config
	engine      *promql.Engine
	queryable   storage.Queryable
	pusher      Pusher
	alertURL    *url.URL
	notifierCfg *config.Config

	scheduler *scheduler
	workerWG  *sync.WaitGroup

	lifecycler *ring.Lifecycler
	ring       *ring.Ring

	// Per-user notifiers with separate queues.
	notifiersMtx sync.Mutex
	notifiers    map[string]*rulerNotifier
}

// NewRuler creates a new ruler from a distributor and chunk store.
func NewRuler(cfg Config, engine *promql.Engine, queryable storage.Queryable, d *distributor.Distributor, rulesAPI client.Client) (*Ruler, error) {
	if cfg.NumWorkers <= 0 {
		return nil, fmt.Errorf("must have at least 1 worker, got %d", cfg.NumWorkers)
	}

	ncfg, err := buildNotifierConfig(&cfg)
	if err != nil {
		return nil, err
	}

	ruler := &Ruler{
		cfg:         cfg,
		engine:      engine,
		queryable:   queryable,
		pusher:      d,
		alertURL:    cfg.ExternalURL.URL,
		notifierCfg: ncfg,
		notifiers:   map[string]*rulerNotifier{},
		workerWG:    &sync.WaitGroup{},
	}

	ruler.scheduler = newScheduler(rulesAPI, cfg.EvaluationInterval, cfg.EvaluationInterval, ruler.newGroup, ruler.removeUser)

	// If sharding is enabled, create/join a ring to distribute tokens to
	// the ruler
	if cfg.EnableSharding {
		ruler.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, ruler, "ruler")
		if err != nil {
			return nil, err
		}

		ruler.ring, err = ring.New(cfg.LifecyclerConfig.RingConfig, "ruler")
		if err != nil {
			return nil, err
		}
	}

	for i := 0; i < cfg.NumWorkers; i++ {
		// initialize each worker in a function that signals when
		// the worker has completed
		ruler.workerWG.Add(1)
		go func() {
			w := newWorker(ruler)
			w.Run()
			ruler.workerWG.Done()
		}()
	}

	go ruler.scheduler.Run()

	level.Info(util.Logger).Log("msg", "ruler up and running")

	return ruler, nil
}

// Stop stops the Ruler.
// Each function of the ruler is terminated before leaving the ring
func (r *Ruler) Stop() {
	r.notifiersMtx.Lock()
	for _, n := range r.notifiers {
		n.stop()
	}
	r.notifiersMtx.Unlock()

	level.Info(util.Logger).Log("msg", "shutting down rules scheduler")
	r.scheduler.Stop()

	level.Info(util.Logger).Log("msg", "waiting for workers to finish")
	r.workerWG.Wait()

	if r.cfg.EnableSharding {
		level.Info(util.Logger).Log("msg", "attempting shutdown lifecycle")
		r.lifecycler.Shutdown()
		level.Info(util.Logger).Log("msg", "shutting down the ring")
		r.ring.Stop()
	}
}

func (r *Ruler) newGroup(userID string, groupName string, rls []rules.Rule) (*group, error) {
	appendable := &appendableAppender{pusher: r.pusher}
	notifier, err := r.getOrCreateNotifier(userID)
	if err != nil {
		return nil, err
	}
	opts := &rules.ManagerOptions{
		Appendable:  appendable,
		QueryFunc:   rules.EngineQueryFunc(r.engine, r.queryable),
		Context:     context.Background(),
		ExternalURL: r.alertURL,
		NotifyFunc:  sendAlerts(notifier, r.alertURL.String()),
		Logger:      util.Logger,
		Metrics:     ruleMetrics,
	}
	return newGroup(groupName, rls, appendable, opts), nil
}

func (r *Ruler) removeUser(userID string) error {
	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	if n, ok := r.notifiers[userID]; ok {
		n.stop()
	}
	delete(r.notifiers, userID)
	return nil
}

// sendAlerts implements a rules.NotifyFunc for a Notifier.
// It filters any non-firing alerts from the input.
//
// Copied from Prometheus's main.go.
func sendAlerts(n *notifier.Manager, externalURL string) rules.NotifyFunc {
	return func(ctx native_ctx.Context, expr string, alerts ...*rules.Alert) {
		var res []*notifier.Alert

		for _, alert := range alerts {
			// Only send actually firing alerts.
			if alert.State == rules.StatePending {
				continue
			}
			a := &notifier.Alert{
				StartsAt:     alert.FiredAt,
				Labels:       alert.Labels,
				Annotations:  alert.Annotations,
				GeneratorURL: externalURL + strutil.TableLinkForExpression(expr),
			}
			if !alert.ResolvedAt.IsZero() {
				a.EndsAt = alert.ResolvedAt
			}
			res = append(res, a)
		}

		if len(alerts) > 0 {
			n.Send(res...)
		}
	}
}

func (r *Ruler) getOrCreateNotifier(userID string) (*notifier.Manager, error) {
	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	n, ok := r.notifiers[userID]
	if ok {
		return n.notifier, nil
	}

	n = newRulerNotifier(&notifier.Options{
		QueueCapacity: r.cfg.NotificationQueueCapacity,
		Do: func(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
			// Note: The passed-in context comes from the Prometheus notifier
			// and does *not* contain the userID. So it needs to be added to the context
			// here before using the context to inject the userID into the HTTP request.
			ctx = user.InjectOrgID(ctx, userID)
			if err := user.InjectOrgIDIntoHTTPRequest(ctx, req); err != nil {
				return nil, err
			}
			// Jaeger complains the passed-in context has an invalid span ID, so start a new root span
			sp := ot.GlobalTracer().StartSpan("notify", ot.Tag{Key: "organization", Value: userID})
			defer sp.Finish()
			ctx = ot.ContextWithSpan(ctx, sp)
			ot.GlobalTracer().Inject(sp.Context(), ot.HTTPHeaders, ot.HTTPHeadersCarrier(req.Header))
			return ctxhttp.Do(ctx, client, req)
		},
	}, util.Logger)

	go n.run()

	// This should never fail, unless there's a programming mistake.
	if err := n.applyConfig(r.notifierCfg); err != nil {
		return nil, err
	}

	r.notifiers[userID] = n
	return n.notifier, nil
}

// Evaluate a list of rules in the given context.
func (r *Ruler) Evaluate(userID string, item *workItem) {
	ctx := user.InjectOrgID(context.Background(), userID)
	logger := util.WithContext(ctx, util.Logger)
	if r.cfg.EnableSharding && !r.ownsRule(item.hash) {
		level.Debug(util.Logger).Log("msg", "ruler: skipping evaluation, not owned", "user_id", item.userID, "group", item.groupName)
		return
	}
	level.Debug(logger).Log("msg", "evaluating rules...", "num_rules", len(item.group.Rules()))
	ctx, cancelTimeout := context.WithTimeout(ctx, r.cfg.GroupTimeout)
	instrument.CollectedRequest(ctx, "Evaluate", evalDuration, nil, func(ctx native_ctx.Context) error {
		if span := ot.SpanFromContext(ctx); span != nil {
			span.SetTag("instance", userID)
			span.SetTag("groupName", item.groupName)
		}
		item.group.Eval(ctx, time.Now())
		return nil
	})
	if err := ctx.Err(); err == nil {
		cancelTimeout() // release resources
	} else {
		level.Warn(logger).Log("msg", "context error", "error", err)
	}

	rulesProcessed.Add(float64(len(item.group.Rules())))
}

func (r *Ruler) ownsRule(hash uint32) bool {
	rlrs, err := r.ring.Get(hash, ring.Read)
	// If an error occurs evaluate a rule as if it is owned
	// better to have extra datapoints for a rule than none at all
	// TODO: add a temporary cache of owned rule values or something to fall back on
	if err != nil {
		level.Warn(util.Logger).Log("msg", "error reading ring to verify rule group ownership", "err", err)
		ringCheckErrors.Inc()
		return true
	}
	if rlrs.Ingesters[0].Addr == r.lifecycler.Addr {
		return true
	}
	level.Debug(util.Logger).Log("msg", "rule group not owned, address does not match", "owner", rlrs.Ingesters[0].Addr, "current", r.cfg.LifecyclerConfig.Addr)
	return false
}

func (r *Ruler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if r.cfg.EnableSharding {
		r.ring.ServeHTTP(w, req)
	} else {
		var unshardedPage = `
			<!DOCTYPE html>
			<html>
				<head>
					<meta charset="UTF-8">
					<title>Cortex Ruler Status</title>
				</head>
				<body>
					<h1>Cortex Ruler Status</h1>
					<p>Ruler running with shards disabled</p>
				</body>
			</html>`
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(unshardedPage))
	}
}
