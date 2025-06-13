package tripperware

import (
	"context"
	"net/http"
	"time"

	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/api"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// RequestResponse contains a request response and the respective request that was used.
type RequestResponse struct {
	Request  Request
	Response Response
}

// DoRequests executes a list of requests in parallel. The limits parameters is used to limit parallelism per single request.
func DoRequests(ctx context.Context, downstream Handler, reqs []Request, limits Limits) ([]RequestResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	// If one of the requests fail, we want to be able to cancel the rest of them.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Feed all requests to a bounded intermediate channel to limit parallelism.
	intermediate := make(chan Request)
	go func() {
		for _, req := range reqs {
			intermediate <- req
		}
		close(intermediate)
	}()

	respChan, errChan := make(chan RequestResponse), make(chan error)
	parallelism := validation.SmallestPositiveIntPerTenant(tenantIDs, limits.MaxQueryParallelism)
	if parallelism > len(reqs) {
		parallelism = len(reqs)
	}
	for i := 0; i < parallelism; i++ {
		go func() {
			for req := range intermediate {
				resp, err := downstream.Do(ctx, req)
				if err != nil {
					errChan <- err
				} else {
					respChan <- RequestResponse{req, resp}
				}
			}
		}()
	}

	resps := make([]RequestResponse, 0, len(reqs))
	var firstErr error
	for range reqs {
		select {
		case resp := <-respChan:
			resps = append(resps, resp)
		case err := <-errChan:
			if firstErr == nil {
				cancel()
				firstErr = err
			}
		}
	}

	return resps, firstErr
}

func SetQueryResponseStats(a *PrometheusResponse, queryStats *stats.QueryStats) {
	if queryStats != nil {
		v := a.Data.Result.GetVector()
		if v != nil {
			queryStats.AddResponseSeries(uint64(len(v.GetSamples())))
			return
		}

		m := a.Data.Result.GetMatrix()
		if m != nil {
			queryStats.AddResponseSeries(uint64(len(m.GetSampleStreams())))
			return
		}
	}
}

func ParseTimeParamMillis(r *http.Request, paramName string, defaultValue time.Time) (int64, error) {
	t, err := api.ParseTimeParam(r, paramName, defaultValue)
	if err != nil {
		return 0, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	return util.TimeToMillis(t), nil
}

func ParseTimeMillis(s string) (int64, error) {
	t, err := api.ParseTime(s)
	if err != nil {
		return 0, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	return util.TimeToMillis(t), nil
}

func ParseDurationMillis(s string) (int64, error) {
	d, err := api.ParseDuration(s)
	if err != nil {
		return 0, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	return int64(d / (time.Millisecond / time.Nanosecond)), nil
}
