package querier

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/users"
)

// Queries are a set of matchers with time ranges - should not get into megabytes
const maxRemoteReadQuerySize = 1024 * 1024

// RemoteReadLimits are the per-tenant limits used by the remote read handler.
type RemoteReadLimits interface {
	// AcceptHASamples returns whether HA samples deduplication is enabled for the tenant.
	AcceptHASamples(userID string) bool
	// HAReplicaLabel returns the label identifying a Prometheus HA replica for the tenant.
	HAReplicaLabel(userID string) string
}

// haReplicaLabels returns the HA replica label names configured for the tenants of the request.
// The replica label is removed by the distributor when HA deduplication is enabled, so it is
// never present on the stored series.
func haReplicaLabels(ctx context.Context, limits RemoteReadLimits) map[string]struct{} {
	if limits == nil {
		return nil
	}
	tenantIDs, err := users.TenantIDs(ctx)
	if err != nil {
		return nil
	}
	var names map[string]struct{}
	for _, id := range tenantIDs {
		if !limits.AcceptHASamples(id) {
			continue
		}
		if name := limits.HAReplicaLabel(id); name != "" {
			if names == nil {
				names = map[string]struct{}{}
			}
			names[name] = struct{}{}
		}
	}
	return names
}

// removeHAReplicaMatchers drops the matchers on the given HA replica label names. Prometheus adds
// its external labels, including the replica label, to remote read matchers. Since that label
// is not stored when HA deduplication is enabled, keeping it would return no data.
// If dropping them would leave no matcher at all, the original matchers are returned.
func removeHAReplicaMatchers(matchers []*labels.Matcher, names map[string]struct{}) []*labels.Matcher {
	if len(names) == 0 {
		return matchers
	}
	out := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		if _, ok := names[m.Name]; !ok {
			out = append(out, m)
		}
	}
	if len(out) == 0 {
		return matchers
	}
	return out
}

// RemoteReadHandler handles Prometheus remote read requests. If limits is not nil, matchers on
// the HA replica label are ignored for tenants having HA deduplication enabled.
func RemoteReadHandler(q storage.Queryable, limits RemoteReadLimits, logger log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req client.ReadRequest
		logger := util_log.WithContext(r.Context(), logger)
		if err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRemoteReadQuerySize, &req, util.RawSnappy); err != nil {
			level.Error(logger).Log("msg", "failed to parse proto", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		replicaLabels := haReplicaLabels(ctx, limits)

		// Fetch samples for all queries in parallel.
		resp := client.ReadResponse{
			Results: make([]*client.QueryResponse, len(req.Queries)),
		}
		errors := make(chan error)
		for i, qr := range req.Queries {
			go func(i int, qr *client.QueryRequest) {
				from, to, matchers, err := client.FromQueryRequest(storecache.NoopMatchersCache, qr)
				if err != nil {
					errors <- err
					return
				}

				matchers = removeHAReplicaMatchers(matchers, replicaLabels)

				querier, err := q.Querier(int64(from), int64(to))
				if err != nil {
					errors <- err
					return
				}

				params := &storage.SelectHints{
					Start: int64(from),
					End:   int64(to),
				}
				seriesSet := querier.Select(ctx, false, params, matchers...)
				resp.Results[i], err = client.SeriesSetToQueryResponse(seriesSet)
				errors <- err
			}(i, qr)
		}

		var lastErr error
		for range req.Queries {
			err := <-errors
			if err != nil {
				lastErr = err
			}
		}
		if lastErr != nil {
			http.Error(w, lastErr.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Add("Content-Type", "application/x-protobuf")
		if err := util.SerializeProtoResponse(w, &resp, util.RawSnappy); err != nil {
			level.Error(logger).Log("msg", "error sending remote read response", "err", err)
		}
	})
}
