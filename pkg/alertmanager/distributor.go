package alertmanager

import (
	"context"
	"hash/fnv"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/tenant"
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

	d.Service = services.NewBasicService(nil, d.running, nil)
	return d, nil
}

func (d *Distributor) running(ctx context.Context) error {
	<-ctx.Done()
	d.requestsInFlight.Wait()
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

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		d.doRead(userID, w, r, logger)
	} else {
		d.doWrite(userID, w, r, logger)
	}
}

func (d *Distributor) doWrite(userID string, w http.ResponseWriter, r *http.Request, logger log.Logger) {
	var body []byte
	var err error
	if r.Body != nil {
		body, err = ioutil.ReadAll(r.Body)
		if err != nil {
			if err.Error() == "http: request body too large\n" {
				http.Error(w, "Request body too large", http.StatusBadRequest)
			}
			level.Error(logger).Log("msg", "failed to read the request body during write", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	var firstSuccessfulResponse *httpgrpc.HTTPResponse
	var respMtx sync.Mutex
	grpcHeaders := httpTogrpchttpHeaders(r.Header)
	err = ring.DoBatch(r.Context(), RingOp, d.alertmanagerRing, []uint32{shardByUser(userID)}, func(am ring.InstanceDesc, _ []int) error {
		// Use a background context to make sure all alertmanagers get the request even if we return early.
		localCtx, cancel := context.WithTimeout(context.Background(), d.cfg.AlertmanagerClient.RemoteTimeout)
		defer cancel()
		localCtx = user.InjectOrgID(localCtx, userID)
		sp, localCtx := opentracing.StartSpanFromContext(localCtx, "DistributeRequest.doWrite")
		defer sp.Finish()

		resp, err := d.doRequest(localCtx, am, &httpgrpc.HTTPRequest{
			Method:  r.Method,
			Url:     r.RequestURI,
			Body:    body,
			Headers: grpcHeaders,
		})
		if err != nil {
			return err
		}

		if resp.Code/100 != 2 {
			return httpgrpc.ErrorFromHTTPResponse(resp)
		}

		respMtx.Lock()
		if firstSuccessfulResponse == nil {
			firstSuccessfulResponse = resp
		}
		respMtx.Unlock()

		return nil
	}, func() {})

	if err != nil {
		respondFromError(err, w, logger)
	}

	respMtx.Lock() // Another request might be ongoing after quorum.
	resp := firstSuccessfulResponse
	respMtx.Unlock()

	if resp != nil {
		respondFromHTTPGRPCResponse(w, resp)
	} else {
		// This should not happen.
		level.Error(logger).Log("msg", "distributor did not receive response from alertmanager though no errors")
		w.WriteHeader(http.StatusInternalServerError)
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

	sp, ctx := opentracing.StartSpanFromContext(ctx, "DistributeRequest.doRead")
	defer sp.Finish()
	// Until we have a mechanism to combine the results from multiple alertmanagers,
	// we forward the request to only only of the alertmanagers.
	amDesc := replicationSet.Ingesters[rand.Intn(len(replicationSet.Ingesters))]
	resp, err := d.doRequest(ctx, amDesc, req)
	if err != nil {
		respondFromError(err, w, logger)
		return
	}

	respondFromHTTPGRPCResponse(w, resp)
}

func respondFromError(err error, w http.ResponseWriter, logger log.Logger) {
	httpResp, ok := httpgrpc.HTTPResponseFromError(errors.Cause(err))
	if !ok {
		level.Error(logger).Log("msg", "failed to process the request to the alertmanager", "err", err)
		http.Error(w, "Failed to process the request to the alertmanager", http.StatusInternalServerError)
		return
	}
	respondFromHTTPGRPCResponse(w, httpResp)
}

func respondFromHTTPGRPCResponse(w http.ResponseWriter, httpResp *httpgrpc.HTTPResponse) {
	for _, h := range httpResp.Headers {
		for _, v := range h.Values {
			w.Header().Add(h.Key, v)
		}
	}
	w.WriteHeader(int(httpResp.Code))
	w.Write(httpResp.Body) //nolint
}

func (d *Distributor) doRequest(ctx context.Context, am ring.InstanceDesc, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	amClient, err := d.alertmanagerClientsPool.GetClientFor(am.Addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get alertmanager client from pool (alertmanager address: %s)", am.Addr)
	}

	return amClient.HandleRequest(ctx, req)
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
