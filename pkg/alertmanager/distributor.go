package alertmanager

import (
	"context"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertmanagerpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// Distributor forwards requests to individual alertmanagers.
type Distributor struct {
	services.Service

	cfg              *MultitenantAlertmanagerConfig
	requestsInFlight sync.WaitGroup

	alertmanagerRing        ring.ReadRing
	alertmanagerClientsPool ClientsPool

	logger log.Logger

	receivedRequests *prometheus.CounterVec
	amSends          *prometheus.CounterVec
	amSendFailures   *prometheus.CounterVec
}

// NewDistributor constructs a new Distributor
func NewDistributor(cfg *MultitenantAlertmanagerConfig, alertmanagersRing *ring.Ring, alertmanagerClientsPool ClientsPool, logger log.Logger, reg prometheus.Registerer) (d *Distributor, err error) {
	if alertmanagerClientsPool == nil {
		alertmanagerClientsPool = newAlertmanagerClientsPool(client.NewRingServiceDiscovery(alertmanagersRing), cfg.AlertmanagerClient, logger, reg)
	}

	d = &Distributor{
		cfg:                     cfg,
		logger:                  logger,
		alertmanagerRing:        alertmanagersRing,
		alertmanagerClientsPool: alertmanagerClientsPool,
	}

	d.receivedRequests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_alertmanager_distributor_received_requests_total",
		Help: "The total number of requests received.",
	}, []string{"user"})
	d.amSends = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_alertmanager_distributor_alertmanager_send_total",
		Help: "The total number of requests sent to the alertmanager.",
	}, []string{"ingester"})
	d.amSendFailures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_alertmanager_distributor_alertmanager_send_failures_total",
		Help: "The total number of requests sent to the alertmanager that failed.",
	}, []string{"ingester"})

	d.Service = services.NewBasicService(d.starting, d.running, d.stopping)
	return d, nil
}

func (d *Distributor) starting(ctx context.Context) error {
	return nil
}

func (d *Distributor) running(ctx context.Context) error {
	<-ctx.Done()
	d.requestsInFlight.Wait()
	return nil
}

func (d *Distributor) stopping(_ error) error {
	return nil
}

// IsPathSupported returns true if the given route is currently supported by the Distributor.
func (d *Distributor) IsPathSupported(path string) bool {
	// API can be found at https://petstore.swagger.io/?url=https://raw.githubusercontent.com/prometheus/alertmanager/master/api/v2/openapi.yaml.
	return strings.HasSuffix(path, "/alerts") ||
		strings.HasSuffix(path, "/alerts/groups")
}

// DistributeRequest shards the writes and returns as soon as the quorum is satisfied.
// In case of reads, it proxies the request to one of the alertmanagers.
// DistributeRequest assumes that the caller has verified IsPathSupported return
// true for the route.
func (d *Distributor) DistributeRequest(w http.ResponseWriter, r *http.Request) {
	d.requestsInFlight.Add(1)
	defer d.requestsInFlight.Done()

	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	logger := log.With(d.logger, "user", userID)

	d.receivedRequests.WithLabelValues(userID).Inc()

	if r.Method == http.MethodGet {
		d.doRead(userID, w, r, logger)
	} else {
		d.doWrite(userID, w, r, logger)
	}
}

func (d *Distributor) doWrite(userID string, w http.ResponseWriter, r *http.Request, logger log.Logger) {
	source := util.GetSourceIPsFromOutgoingCtx(r.Context())

	var body []byte
	var err error
	if r.Body != nil {
		body, err = ioutil.ReadAll(r.Body)
		if err != nil {
			level.Error(logger).Log("msg", "failed to read the request body during write", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	firstSuccessfulResponse := make(chan *alertmanagerpb.Response, 1)
	grpcHeaders := httpTogrpchttpHeaders(r.Header)
	err = ring.DoBatch(r.Context(), RingOp, d.alertmanagerRing, []uint32{shardByUser(userID)}, func(am ring.InstanceDesc, _ []int) error {
		d.amSends.WithLabelValues(am.Addr).Inc()

		// Use a background context to make sure all alertmanagers get the request even if we return early.
		localCtx, cancel := context.WithTimeout(context.Background(), d.cfg.AlertmanagerClient.RemoteTimeout)
		defer cancel()
		localCtx = user.InjectOrgID(localCtx, userID)
		if sp := opentracing.SpanFromContext(r.Context()); sp != nil {
			localCtx = opentracing.ContextWithSpan(localCtx, sp)
		}
		// Get clientIP(s) from Context and add it to localCtx
		localCtx = util.AddSourceIPsToOutgoingContext(localCtx, source)

		resp, err := d.doRequest(localCtx, am, &httpgrpc.HTTPRequest{
			Method:  r.Method,
			Url:     r.RequestURI,
			Body:    body,
			Headers: grpcHeaders,
		})
		if err != nil {
			d.amSendFailures.WithLabelValues(am.Addr).Inc()
			return err
		}

		if resp.HttpResponse.Code/100 != 2 {
			return httpgrpc.ErrorFromHTTPResponse(resp.HttpResponse)
		}

		select {
		case firstSuccessfulResponse <- resp:
		default:
		}

		return nil
	}, func() {})

	if err != nil {
		d.respondFromError(err, w, logger)
	}

	select {
	case resp := <-firstSuccessfulResponse:
		d.respondFromGRPCHTTPResponse(w, resp.HttpResponse)
	default:
		// This should not happen, but we have this default case to prevent
		// and bugs deadlocking at this point.
		level.Warn(logger).Log("msg", "first successful response not available on nil error")
		w.WriteHeader(http.StatusOK)
	}
}

func (d *Distributor) doRead(userID string, w http.ResponseWriter, r *http.Request, logger log.Logger) {
	key := shardByUser(userID)
	replicationSet, err := d.alertmanagerRing.Get(key, RingOp, nil, nil, nil)
	if err != nil {
		level.Error(logger).Log("msg", "failed to get replication set from the ring", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	req, err := server.HTTPRequest(r)
	if err != nil {
		level.Error(logger).Log("msg", "failed to get grpc request from http request", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), d.cfg.AlertmanagerClient.RemoteTimeout)
	defer cancel()
	// Until we have a mechanism to combine the results from multiple alertmanagers,
	// we forward the request to only only of the alertmanagers.
	amDesc := replicationSet.Ingesters[0]
	d.amSends.WithLabelValues(amDesc.Addr).Inc()
	resp, err := d.doRequest(ctx, amDesc, req)
	if err != nil {
		d.amSendFailures.WithLabelValues(amDesc.Addr).Inc()
		d.respondFromError(err, w, logger)
		return
	}

	http.Error(w, string(resp.HttpResponse.Body), int(resp.HttpResponse.Code))
}

func (d *Distributor) respondFromError(err error, w http.ResponseWriter, logger log.Logger) {
	httpResp, ok := httpgrpc.HTTPResponseFromError(errors.Cause(err))
	if !ok {
		level.Error(logger).Log("msg", "Failed to process the request to the alertmanager", "err", err)
		http.Error(w, "Failed to process the request to the alertmanager", http.StatusInternalServerError)
		return
	}
	d.respondFromGRPCHTTPResponse(w, httpResp)
}

func (d *Distributor) respondFromGRPCHTTPResponse(w http.ResponseWriter, httpResp *httpgrpc.HTTPResponse) {
	for _, h := range httpResp.Headers {
		for _, v := range h.Values {
			w.Header().Add(h.Key, v)
		}
	}
	http.Error(w, string(httpResp.Body), int(httpResp.Code))
}

func (d *Distributor) doRequest(ctx context.Context, am ring.InstanceDesc, req *httpgrpc.HTTPRequest) (*alertmanagerpb.Response, error) {
	amClient, err := d.alertmanagerClientsPool.GetClientFor(am.Addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get alertmanager from pool %s", am.Addr)
	}

	resp, err := amClient.HandleRequest(ctx, &alertmanagerpb.Request{
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

func httpTogrpchttpHeaders(hs http.Header) []*httpgrpc.Header {
	result := make([]*httpgrpc.Header, 0, len(hs))
	for k, vs := range hs {
		result = append(result, &httpgrpc.Header{
			Key:    k,
			Values: vs,
		})
	}
	return result
}
