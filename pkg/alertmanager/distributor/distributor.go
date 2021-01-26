package distributor

import (
	"context"
	"flag"
	"net/http"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertmanagerpb"

	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/util/tls"

	"github.com/cortexproject/cortex/pkg/ring/kv"

	"github.com/cortexproject/cortex/pkg/alertmanager"

	"github.com/weaveworks/common/httpgrpc/server"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/services"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// AlertmanagerSet is the interface used to get the clients to write/read to a set of alertmanagers.
type AlertmanagerSet interface {
	services.Service

	// GetClientsFor returns the alertmanager clients that should be used to
	// write/read to a particular set of alertmanagers for a given user.
	GetClientsFor(userID string) ([]AlertmanagerClient, error)
}

// Config contains the configuration required to
// create a Distributor
type Config struct {
	RemoteTimeout      time.Duration    `yaml:"remote_timeout"`
	AlertmanagerClient tls.ClientConfig `yaml:"alertmanager_client"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.AlertmanagerClient.RegisterFlagsWithPrefix("alertmanager-distributor.alertmanager-client", f)
	f.DurationVar(&cfg.RemoteTimeout, "alertmanager-distributor.remote-timeout", 2*time.Second, "Timeout for downstream alertmanagers.")
}

// Distributor forwards requests to individual alertmanagers.
type Distributor struct {
	services.Service

	cfg              Config
	requestsInFlight sync.WaitGroup

	alertmanagers AlertmanagerSet

	// Manager for subservices (AM Ring)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	logger log.Logger

	receivedRequests  *prometheus.CounterVec
	amSends           *prometheus.CounterVec
	amSendFailures    *prometheus.CounterVec
	replicationFactor prometheus.Gauge
}

// New constructs a new Distributor
func New(cfg Config, amConfig alertmanager.MultitenantAlertmanagerConfig, ringService services.Service, r prometheus.Registerer, logger log.Logger, reg prometheus.Registerer) (d *Distributor, err error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	d = &Distributor{
		cfg:    cfg,
		logger: logger,
	}

	// Initialize the alertmanager ring.
	amRingCfg := amConfig.ShardingRing.ToRingConfig()
	alertmanagerssRingBackend, err := kv.NewClient(
		amRingCfg.KVStore,
		ring.GetCodec(),
		kv.RegistererWithKVName(reg, "distributor-alertmanager"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create alertmanager ring backend")
	}

	alertmanagersRing, err := ring.NewWithStoreClientAndStrategy(amRingCfg, alertmanager.RingNameForClient, alertmanager.RingKey, alertmanagerssRingBackend, ring.NewIgnoreUnhealthyInstancesReplicationStrategy())
	if err != nil {
		return nil, errors.Wrap(err, "failed to create alertmanager ring client")
	}

	if reg != nil {
		reg.MustRegister(alertmanagersRing)
	}

	alertmanagers, err := newAlertmanagerReplicationSet(alertmanagersRing, cfg.AlertmanagerClient, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create alertmanager set")
	}

	d.alertmanagers = alertmanagers
	d.initMetrics(r)
	d.replicationFactor.Set(float64(alertmanagersRing.ReplicationFactor()))

	d.subservices, err = services.NewManager(ringService)
	if err != nil {
		return nil, err
	}
	d.subservicesWatcher = services.NewFailureWatcher()
	d.subservicesWatcher.WatchManager(d.subservices)

	d.Service = services.NewBasicService(d.starting, d.running, d.stopping)
	return d, nil
}

func (d *Distributor) initMetrics(r prometheus.Registerer) {
	d.receivedRequests = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "alertmanager_distributor_received_requests_total",
		Help:      "The total number of requests received.",
	}, []string{"user"})
	d.amSends = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "alertmanager_distributor_alertmanager_send_total",
		Help:      "The total number of requests sent to alertmanager.",
	}, []string{"ingester"})
	d.amSendFailures = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "alertmanager_distributor_alertmanager_send_failures_total",
		Help:      "The total number of requests failed to send to alertmanager.",
	}, []string{"ingester"})
	d.replicationFactor = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "alertmanager_distributor_replication_factor",
		Help:      "The configured replication factor.",
	})
}

func (d *Distributor) starting(ctx context.Context) error {
	return services.StartManagerAndAwaitHealthy(ctx, d.subservices)
}

func (d *Distributor) running(ctx context.Context) error {
	<-ctx.Done()
	d.requestsInFlight.Wait()
	return nil
}

func (d *Distributor) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), d.subservices)
}

func (d *Distributor) ServeHTTPAsGRPC(w http.ResponseWriter, r *http.Request) {
	//TODO: Initialise tracing here.

	d.requestsInFlight.Add(1)
	defer d.requestsInFlight.Done()

	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	req, err := server.HTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	clients, err := d.alertmanagers.GetClientsFor(userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var (
		//TODO: This is probably not the right context
		g, gCtx  = errgroup.WithContext(r.Context())
		mtx      = sync.Mutex{}
		sucesses int
	)
	for _, c := range clients {
		// Change variables scope since it will be used in a goroutine.
		c := c
		g.Go(func() error {
			resp, err := c.HandleWrite(gCtx, &alertmanagerpb.WriteRequest{
				UserID:      userID,
				HttpRequest: req,
			})

			if err != nil {
				return errors.Wrapf(err, "failed to proxy request to alertmanager #{c.RemoteAddress()}")
			}
			if resp.GetStatus() == alertmanagerpb.OK {
				mtx.Lock()
				sucesses++
				mtx.Unlock()
			}

			return nil
		})
	}
}

//
//// ServeHTTP forwards the requests to the appropriate alertmanagers.
//func (d *Distributor) ServeHTTP(w http.ResponseWriter, req *http.Request) {
//	d.requestsInFlight.Add(1)
//	defer d.requestsInFlight.Done()
//
//	userID, err := tenant.TenantID(req.Context())
//	if err != nil {
//		http.Error(w, err.Error(), http.StatusUnauthorized)
//		return
//	}
//	level.Debug(d.logger).Log("msg", "alertmanager distributor request", "path", req.URL.Path, "user", userID)
//	d.receivedRequests.WithLabelValues(userID).Inc()
//	// TODO: Add some limits.
//
//	// TODO(codesome): Do any validation here so that AMs don't send any 4xx.
//	// This is tricky because some AM might not have the user and some might have.
//
//	// http.Request.Clone() does not do a deep copy, hence copying the body.
//	// https://github.com/golang/go/issues/36095#issuecomment-568239806.
//	var b bytes.Buffer
//	if req.Body != nil {
//		if _, err := b.ReadFrom(req.Body); err != nil {
//			level.Error(d.logger).Log("msg", "Error reading request body", "user", userID, "err", err)
//			w.WriteHeader(http.StatusBadRequest)
//			return
//		}
//		req.Body = ioutil.NopCloser(&b)
//	}
//
//	callback := func(am ring.IngesterDesc) (retErr error) {
//		d.amSends.WithLabelValues(am.Addr).Inc()
//		defer func() {
//			if retErr != nil {
//				d.amSendFailures.WithLabelValues(am.Addr).Inc()
//			}
//		}()
//
//		reqURL, err := url.Parse("http://" + path.Join(am.Addr, req.URL.Path))
//		if err != nil {
//			return errors.Wrap(err, "creating alertmanager URL")
//		}
//
//		// Use a background context to make sure all alertmanagers get alerts even if we return early.
//		localCtx, cancel := context.WithTimeout(context.Background(), d.cfg.RemoteTimeout)
//		defer cancel()
//		localCtx = user.InjectOrgID(localCtx, userID)
//		if sp := opentracing.SpanFromContext(req.Context()); sp != nil {
//			localCtx = opentracing.ContextWithSpan(localCtx, sp)
//		}
//
//		newReq := req.Clone(localCtx)
//		newReq.RequestURI = ""
//		newReq.URL = reqURL
//		if req.Body != nil {
//			newReq.Body = ioutil.NopCloser(bytes.NewReader(b.Bytes()))
//		}
//
//		resp, err := d.client.Do(newReq)
//		defer func() {
//			if err != nil || resp.Body == nil {
//				return
//			}
//			if err := resp.Body.Close(); err != nil {
//				level.Error(d.logger).Log("msg", "Error closing alertmanager response body", "user", userID, "err", err)
//			}
//		}()
//
//		if err != nil {
//			return err
//		}
//
//		if resp.StatusCode/100 != 2 {
//			return &amReqError{status: resp.StatusCode}
//		}
//
//		return nil
//	}
//
//	if strings.HasPrefix(req.URL.Path, d.alertmanagerHTTPPrefix) {
//		// Only requests with alertmanager prefix are for tenant specific alertmanger.
//		// Hence we only shard them.
//		key := shardByUser(userID)
//		err = ring.DoBatch(req.Context(), ring.Write, d.amRing, []uint32{key}, func(am ring.IngesterDesc, _ []int) error {
//			return callback(am)
//		}, func() {})
//	} else {
//		// TODO(codesome): other modules would be sending the config change, etc,
//		// to all alertmanagers right now. Change it to send to _one_ distributor.
//		// It could be just a config change and no code changes required.
//		err = ring.DoAll(req.Context(), d.amRing, callback, func() {})
//	}
//
//	if err == nil {
//		w.WriteHeader(http.StatusOK)
//		return
//	}
//	if amErr, ok := err.(*amReqError); ok {
//		w.WriteHeader(amErr.status)
//		return
//	}
//	level.Error(d.logger).Log("msg", "Error forwarding requests to alertmanager", "user", userID, "err", err)
//	w.WriteHeader(http.StatusInternalServerError)
//}
//
//type amReqError struct {
//	status int
//}
//
//func (e amReqError) Error() string {
//	if e.status != 0 {
//		return fmt.Sprintf("status code %d", e.status)
//	}
//	return ""
//}
