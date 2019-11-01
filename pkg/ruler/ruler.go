package ruler

import (
	native_ctx "context"
	"flag"
	"hash/fnv"
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
	promRules "github.com/prometheus/prometheus/rules"
	promStorage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/strutil"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
	store "github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/weaveworks/common/user"
)

var (
	ringCheckErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "ruler_ring_check_errors_total",
		Help:      "Number of errors that have occurred when checking the ring for ownership",
	})
	configUpdatesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "ruler_config_updates_total",
		Help:      "Total number of config updates triggered by a user",
	}, []string{"user"})
)

// Config is the configuration for the recording rules server.
type Config struct {
	ExternalURL        flagext.URLValue // This is used for template expansion in alerts; must be a valid URL
	EvaluationInterval time.Duration    // How frequently to evaluate rules by default.
	PollInterval       time.Duration    // How frequently to poll for updated rules
	StoreConfig        RuleStoreConfig  // Rule Storage and Polling configuration
	RulePath           string           // Path to store rule files for prom manager

	AlertmanagerURL             flagext.URLValue // URL of the Alertmanager to send notifications to.
	AlertmanagerDiscovery       bool             // Whether to use DNS SRV records to discover alertmanagers.
	AlertmanagerRefreshInterval time.Duration    // How long to wait between refreshing the list of alertmanagers based on DNS service discovery.
	AlertmanangerEnableV2API    bool             // Enables the ruler notifier to use the alertmananger V2 API
	NotificationQueueCapacity   int              // Capacity of the queue for notifications to be sent to the Alertmanager.
	NotificationTimeout         time.Duration    // HTTP timeout duration when sending notifications to the Alertmanager.

	EnableSharding   bool // Enable sharding rule groups
	SearchPendingFor time.Duration
	LifecyclerConfig ring.LifecyclerConfig
	FlushCheckPeriod time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.LifecyclerConfig.RegisterFlagsWithPrefix("ruler.", f)
	cfg.StoreConfig.RegisterFlags(f)

	// Deprecated Flags that will be maintained to avoid user disruption
	flagext.DeprecatedFlag(f, "ruler.client-timeout", "This flag has been renamed to ruler.configs.client-timeout")
	flagext.DeprecatedFlag(f, "ruler.group-timeout", "This flag is no longer functional.")
	flagext.DeprecatedFlag(f, "ruler.num-workers", "This flag is no longer functional. For increased concurrency horizontal sharding is recommended")

	cfg.ExternalURL.URL, _ = url.Parse("") // Must be non-nil
	f.Var(&cfg.ExternalURL, "ruler.external.url", "URL of alerts return path.")
	f.DurationVar(&cfg.EvaluationInterval, "ruler.evaluation-interval", 1*time.Minute, "How frequently to evaluate rules")
	f.DurationVar(&cfg.PollInterval, "ruler.poll-interval", 1*time.Minute, "How frequently to poll for rule changes")
	f.Var(&cfg.AlertmanagerURL, "ruler.alertmanager-url", "URL of the Alertmanager to send notifications to.")
	f.BoolVar(&cfg.AlertmanagerDiscovery, "ruler.alertmanager-discovery", false, "Use DNS SRV records to discover alertmanager hosts.")
	f.DurationVar(&cfg.AlertmanagerRefreshInterval, "ruler.alertmanager-refresh-interval", 1*time.Minute, "How long to wait between refreshing alertmanager hosts.")
	f.BoolVar(&cfg.AlertmanangerEnableV2API, "ruler.alertmanager-use-v2", false, "If enabled requests to alertmanager will utilize the V2 API.")
	f.IntVar(&cfg.NotificationQueueCapacity, "ruler.notification-queue-capacity", 10000, "Capacity of the queue for notifications to be sent to the Alertmanager.")
	f.DurationVar(&cfg.NotificationTimeout, "ruler.notification-timeout", 10*time.Second, "HTTP timeout duration when sending notifications to the Alertmanager.")
	if flag.Lookup("promql.lookback-delta") == nil {
		flag.DurationVar(&promql.LookbackDelta, "promql.lookback-delta", promql.LookbackDelta, "Time since the last sample after which a time series is considered stale and ignored by expression evaluations.")
	}
	f.DurationVar(&cfg.SearchPendingFor, "ruler.search-pending-for", 5*time.Minute, "Time to spend searching for a pending ruler when shutting down.")
	f.BoolVar(&cfg.EnableSharding, "ruler.enable-sharding", false, "Distribute rule evaluation using ring backend")
	f.DurationVar(&cfg.FlushCheckPeriod, "ruler.flush-period", 1*time.Minute, "Period with which to attempt to flush rule groups.")
	f.StringVar(&cfg.RulePath, "ruler.rule-path", "/rules", "file path to store temporary rule files for the prometheus rule managers")
}

// Ruler evaluates rules.
type Ruler struct {
	cfg         Config
	engine      *promql.Engine
	queryable   promStorage.Queryable
	pusher      Pusher
	alertURL    *url.URL
	notifierCfg *config.Config

	lifecycler *ring.Lifecycler
	ring       *ring.Ring

	store          rules.RuleStore
	mapper         *mapper
	userManagerMtx sync.Mutex
	userManagers   map[string]*promRules.Manager

	// Per-user notifiers with separate queues.
	notifiersMtx sync.Mutex
	notifiers    map[string]*rulerNotifier

	done       chan struct{}
	terminated chan struct{}
}

// NewRuler creates a new ruler from a distributor and chunk store.
func NewRuler(cfg Config, engine *promql.Engine, queryable promStorage.Queryable, d *distributor.Distributor) (*Ruler, error) {
	ncfg, err := buildNotifierConfig(&cfg)
	if err != nil {
		return nil, err
	}

	ruleStore, err := NewRuleStorage(cfg.StoreConfig)
	if err != nil {
		return nil, err
	}

	ruler := &Ruler{
		cfg:          cfg,
		engine:       engine,
		queryable:    queryable,
		alertURL:     cfg.ExternalURL.URL,
		notifierCfg:  ncfg,
		notifiers:    map[string]*rulerNotifier{},
		store:        ruleStore,
		pusher:       d,
		mapper:       newMapper(cfg.RulePath),
		userManagers: map[string]*promRules.Manager{},
		done:         make(chan struct{}),
		terminated:   make(chan struct{}),
	}

	// If sharding is enabled, create/join a ring to distribute tokens to
	// the ruler
	if cfg.EnableSharding {
		ruler.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, ruler, "ruler")
		if err != nil {
			return nil, err
		}

		ruler.lifecycler.Start()

		ruler.ring, err = ring.New(cfg.LifecyclerConfig.RingConfig, "ruler")
		if err != nil {
			return nil, err
		}
	}

	go ruler.run()
	level.Info(util.Logger).Log("msg", "ruler up and running")

	return ruler, nil
}

// Stop stops the Ruler.
// Each function of the ruler is terminated before leaving the ring
func (r *Ruler) Stop() {
	close(r.done)
	<-r.terminated

	r.notifiersMtx.Lock()
	for _, n := range r.notifiers {
		n.stop()
	}
	r.notifiersMtx.Unlock()

	if r.cfg.EnableSharding {
		level.Info(util.Logger).Log("msg", "attempting shutdown lifecycle")
		r.lifecycler.Shutdown()
		level.Info(util.Logger).Log("msg", "shutting down the ring")
		r.ring.Stop()
	}

	level.Info(util.Logger).Log("msg", "stopping user managers")
	wg := sync.WaitGroup{}
	r.userManagerMtx.Lock()
	for user, manager := range r.userManagers {
		level.Debug(util.Logger).Log("msg", "shutting down user  manager", "user", user)
		wg.Add(1)
		go func(manager *promRules.Manager, user string) {
			manager.Stop()
			wg.Done()
			level.Debug(util.Logger).Log("msg", "user manager shut down", "user", user)
		}(manager, user)
	}
	wg.Wait()
	r.userManagerMtx.Unlock()
	level.Info(util.Logger).Log("msg", "all user managers stopped")
}

// sendAlerts implements a rules.NotifyFunc for a Notifier.
// It filters any non-firing alerts from the input.
//
// Copied from Prometheus's main.go.
func sendAlerts(n *notifier.Manager, externalURL string) promRules.NotifyFunc {
	return func(ctx native_ctx.Context, expr string, alerts ...*promRules.Alert) {
		var res []*notifier.Alert

		for _, alert := range alerts {
			// Only send actually firing alerts.
			if alert.State == promRules.StatePending {
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

func (r *Ruler) ownsRule(hash uint32) (bool, error) {
	rlrs, err := r.ring.Get(hash, ring.Read, []ring.IngesterDesc{})
	if err != nil {
		level.Warn(util.Logger).Log("msg", "error reading ring to verify rule group ownership", "err", err)
		ringCheckErrors.Inc()
		return false, err
	}
	if rlrs.Ingesters[0].Addr == r.lifecycler.Addr {
		level.Debug(util.Logger).Log("msg", "rule group owned", "owner_addr", rlrs.Ingesters[0].Addr, "addr", r.lifecycler.Addr)
		return true, nil
	}
	level.Debug(util.Logger).Log("msg", "rule group not owned, address does not match", "owner_addr", rlrs.Ingesters[0].Addr, "addr", r.lifecycler.Addr)
	return false, nil
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
		_, err := w.Write([]byte(unshardedPage))
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to serve status page", "err", err)
		}
	}
}

func (r *Ruler) run() {
	defer close(r.terminated)

	tick := time.NewTicker(r.cfg.PollInterval)
	defer tick.Stop()

	r.loadRules(context.Background())
	for {
		select {
		case <-r.done:
			return
		case <-tick.C:
			r.loadRules(context.Background())
		}
	}
}

func (r *Ruler) loadRules(ctx context.Context) {
	ringHasher := fnv.New32a()

	configs, err := r.store.ListAllRuleGroups(ctx)
	if err != nil {
		level.Error(util.Logger).Log("msg", "unable to poll for rules", "err", err)
		return
	}

	// Iterate through each users configuration and determine if the on-disk
	// configurations need to be updated
	for user, cfg := range configs {
		filteredGroups := store.RuleGroupList{}

		// If sharding is enabled, prune the rule group to only contain rules
		// this ruler is responsible for.
		if r.cfg.EnableSharding {
			for _, g := range cfg {
				id := g.User + "/" + g.Namespace + "/" + g.Name
				ringHasher.Reset()
				_, err = ringHasher.Write([]byte(id))
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to create group for user", "user", user, "namespace", g.Namespace, "group", g.Name, "err", err)
					continue
				}
				hash := ringHasher.Sum32()
				owned, err := r.ownsRule(hash)
				if err != nil {
					level.Error(util.Logger).Log("msg", "unable to verify rule group ownership ownership, will retry on the next poll", "err", err)
					return
				}
				if owned {
					filteredGroups = append(filteredGroups, g)
				}
			}
		} else {
			filteredGroups = cfg
		}

		// Map the files to disk and return the file names to be passed to the users manager
		update, files, err := r.mapper.MapRules(user, filteredGroups.Formatted())
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to map rule files", "user", user, "err", err)
			continue
		}

		if update {
			configUpdatesTotal.WithLabelValues(user).Inc()
			r.userManagerMtx.Lock()
			manager, exists := r.userManagers[user]
			r.userManagerMtx.Unlock()
			if !exists {
				manager, err = r.newManager(ctx, user)
				if err != nil {
					level.Error(util.Logger).Log("msg", "unable to create rule manager", "user", user, "err", err)
					continue
				}
				manager.Run()

				r.userManagerMtx.Lock()
				r.userManagers[user] = manager
				r.userManagerMtx.Unlock()
			}
			err = manager.Update(r.cfg.EvaluationInterval, files, nil)
			if err != nil {
				level.Error(util.Logger).Log("msg", "unable to create rule manager", "user", user, "err", err)
				continue
			}
		}
	}

	// Check for deleted users and remove them
	r.userManagerMtx.Lock()
	defer r.userManagerMtx.Unlock()
	for user, mngr := range r.userManagers {
		if _, exists := configs[user]; !exists {
			go mngr.Stop()
			delete(r.userManagers, user)
			level.Info(util.Logger).Log("msg", "deleting rule manager", "user", user)
		}
	}

}

// newManager creates a prometheus rule manager wrapped with a user id
// configured storage, appendable, notifier, and instrumentation
func (r *Ruler) newManager(ctx context.Context, userID string) (*promRules.Manager, error) {
	db := &tsdb{
		appender: &appendableAppender{
			pusher: r.pusher,
			userID: userID,
		},
		queryable: r.queryable,
	}

	notifier, err := r.getOrCreateNotifier(userID)
	if err != nil {
		return nil, err
	}

	// Wrap registerer with userID and cortex_ prefix
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"user": userID}, prometheus.DefaultRegisterer)
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)

	opts := &promRules.ManagerOptions{
		Appendable:  db,
		TSDB:        db,
		QueryFunc:   promRules.EngineQueryFunc(r.engine, r.queryable),
		Context:     user.InjectOrgID(ctx, userID),
		ExternalURL: r.alertURL,
		NotifyFunc:  sendAlerts(notifier, r.alertURL.String()),
		Logger:      util.Logger,
		Registerer:  reg,
	}
	return promRules.NewManager(opts), nil
}
