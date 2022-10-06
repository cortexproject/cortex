package tripperware

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gogo/status"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// RequestResponse contains a request response and the respective request that was used.
type RequestResponse struct {
	Request  Request
	Response Response
}

// DoRequests executes a list of requests in parallel. The limits parameters is used to limit parallelism per single request.
func DoRequests(ctx context.Context, downstream Handler, reqs []Request, limits Limits) ([]RequestResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}
	respChan, errChan := DoRequestsCommon(ctx, tenantIDs, downstream, reqs, limits)

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

// DoRequestsAndMerge executes a list of requests in parallel. The limits parameters is used to limit parallelism per single request.
func DoRequestsAndMerge(ctx context.Context, downstream Handler, reqs []Request, limits Limits, merger Merger, emptyResp func() Response) (Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}
	respChan, errChan := DoRequestsCommon(ctx, tenantIDs, downstream, reqs, limits)

	finalResp := emptyResp()
	var firstErr error
	for range reqs {
		select {
		case resp := <-respChan:
			finalResp, err = merger.MergeResponse(finalResp, resp.Response)
			if err != nil {
				return nil, err
			}
		case err := <-errChan:
			if firstErr == nil {
				cancel()
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}

	return finalResp, nil
}

func DoRequestsCommon(ctx context.Context, tenantIDs []string, downstream Handler, reqs []Request, limits Limits) (chan RequestResponse, chan error) {
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

	return respChan, errChan
}

func DecorateWithParamName(err error, field string) error {
	errTmpl := "invalid parameter %q; %v"
	if status, ok := status.FromError(err); ok {
		return httpgrpc.Errorf(int(status.Code()), errTmpl, field, status.Message())
	}
	return fmt.Errorf(errTmpl, field, err)
}

func ParseTimeParam(r *http.Request, paramName string, defaultValue int64) (int64, error) {
	val := r.FormValue(paramName)
	if val == "" {
		val = strconv.FormatInt(defaultValue, 10)
	}
	result, err := util.ParseTime(val)
	if err != nil {
		return 0, errors.Wrapf(err, "Invalid time value for '%s'", paramName)
	}
	return result, nil
}
