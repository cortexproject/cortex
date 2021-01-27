package distributor

import (
	"context"
	"flag"
	"hash/fnv"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/user"

	am_client "github.com/cortexproject/cortex/pkg/alertmanager/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/tls"
)

// Config contains the configuration required to
// create a Distributor
type Config struct {
	RemoteTimeout      time.Duration    `yaml:"remote_timeout"`
	AlertmanagerClient tls.ClientConfig `yaml:"alertmanager_client"`

	// For testing and for extending the ingester by adding calls to the client
	AlertmanagerClientFactory client.PoolFactory `yaml:"-"`
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

	alertmanagerRing ring.ReadRing
	alertmanagerPool *client.Pool
	replication      int

	// Manager for subservices (AlertmanagerSet)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	logger log.Logger

	receivedRequests  *prometheus.CounterVec
	amSends           *prometheus.CounterVec
	amSendFailures    *prometheus.CounterVec
	replicationFactor prometheus.Gauge
}

// New constructs a new Distributor
func New(cfg Config, alertmanagersRing *ring.Ring, logger log.Logger, reg prometheus.Registerer) (d *Distributor, err error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	if cfg.AlertmanagerClientFactory == nil {
		cfg.AlertmanagerClientFactory = newAlertmanagerClientFactory(cfg.AlertmanagerClient, reg)
	}

	d = &Distributor{
		cfg:    cfg,
		logger: logger,
	}

	d.alertmanagerRing = alertmanagersRing
	d.alertmanagerPool = newAlertmanagerClientPool(client.NewRingServiceDiscovery(alertmanagersRing), cfg.AlertmanagerClientFactory, logger, reg)
	d.replication = alertmanagersRing.ReplicationFactor()

	d.initMetrics(reg)
	d.replicationFactor.Set(float64(alertmanagersRing.ReplicationFactor()))

	d.subservices, err = services.NewManager(alertmanagersRing, d.alertmanagerPool)
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

func (d *Distributor) DistributeRequest(w http.ResponseWriter, r *http.Request) {
	//TODO: Initialise tracing here.

	d.requestsInFlight.Add(1)
	defer d.requestsInFlight.Done()

	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	if r.Method == http.MethodGet {
		d.doRead(userID, w, r)
	} else {
		d.doWrite(userID, w, r)
	}
}

func (d *Distributor) doWrite(userID string, w http.ResponseWriter, r *http.Request) {
	source := util.GetSourceIPsFromOutgoingCtx(r.Context())

	err := ring.DoBatch(r.Context(), ring.Write, d.alertmanagerRing, []uint32{shardByUser(userID)}, func(am ring.IngesterDesc, _ []int) error {
		// Use a background context to make sure all alertmanagers get the request even if we return early.
		localCtx, cancel := context.WithTimeout(context.Background(), d.cfg.RemoteTimeout)
		defer cancel()
		localCtx = user.InjectOrgID(localCtx, userID)
		if sp := opentracing.SpanFromContext(r.Context()); sp != nil {
			localCtx = opentracing.ContextWithSpan(localCtx, sp)
		}
		// Get clientIP(s) from Context and add it to localCtx
		localCtx = util.AddSourceIPsToOutgoingContext(localCtx, source)

		resp, err := d.doRequest(localCtx, am, userID, r)
		if err != nil {
			return err
		}

		if resp.GetStatus() != am_client.OK {
			return errors.New("alertmanager grpc request not ok")
		}

		return nil
	}, func() {})

	if err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (d *Distributor) doRead(userID string, w http.ResponseWriter, r *http.Request) {
	key := shardByUser(userID)
	replicationSet, err := d.alertmanagerRing.Get(key, ring.Read, nil, nil, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
	}

	// Until we have a mechanism to combine the results from multiple alertmanagers,
	// we forward the request to only only of the alertmanagers.
	resp, err := d.doRequest(r.Context(), replicationSet.Ingesters[0], userID, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	if resp.GetStatus() != am_client.OK {
		w.WriteHeader(http.StatusInternalServerError)
	}

	if int(resp.HttpResponse.Code) != http.StatusOK {
		w.WriteHeader(int(resp.HttpResponse.Code))
		return
	}

	_, err = w.Write(resp.HttpResponse.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (d *Distributor) doRequest(ctx context.Context, am ring.IngesterDesc, userID string, r *http.Request) (*am_client.Response, error) {
	c, err := d.alertmanagerPool.GetClientFor(am.Addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get alertmanager from pool %s", am.Addr)
	}

	req, err := server.HTTPRequest(r)
	if err != nil {
		return nil, errors.Wrap(err, "create server HTTPRequest")
	}

	amClient := c.(AlertmanagerClient)
	resp, err := amClient.HandleRequest(ctx, &am_client.Request{
		UserID:      userID,
		HttpRequest: req,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to proxy request to alertmanager %s", amClient.RemoteAddress())
	}

	return resp, err
}

func shardByUser(userID string) uint32 {
	ringHasher := fnv.New32a()
	// Hasher never returns err.
	_, _ = ringHasher.Write([]byte(userID))
	return ringHasher.Sum32()
}
