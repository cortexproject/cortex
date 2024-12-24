package tenantfederation

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// NewExemplarQueryable returns a exemplarQueryable that iterates through all the
// tenant IDs that are part of the request and aggregates the results from each
// tenant's ExemplarQuerier by sending of subsequent requests.
// By setting byPassWithSingleQuerier to true the mergeExemplarQuerier gets by-passed
// and results for request with a single exemplar querier will not contain the
// "__tenant_id__" label. This allows a smoother transition, when enabling
// tenant federation in a cluster.
// The result contains a label "__tenant_id__" to identify the tenant ID that
// it originally resulted from.
// If the label "__tenant_id__" is already existing, its value is overwritten
// by the tenant ID and the previous value is exposed through a new label
// prefixed with "original_". This behaviour is not implemented recursively.
func NewExemplarQueryable(upstream storage.ExemplarQueryable, maxConcurrent int, byPassWithSingleQuerier bool, reg prometheus.Registerer) storage.ExemplarQueryable {
	return NewMergeExemplarQueryable(defaultTenantLabel, maxConcurrent, tenantExemplarQuerierCallback(upstream), byPassWithSingleQuerier, reg)
}

func tenantExemplarQuerierCallback(exemplarQueryable storage.ExemplarQueryable) MergeExemplarQuerierCallback {
	return func(ctx context.Context) ([]string, []storage.ExemplarQuerier, error) {
		tenantIDs, err := tenant.TenantIDs(ctx)
		if err != nil {
			return nil, nil, err
		}

		var queriers = make([]storage.ExemplarQuerier, len(tenantIDs))
		for pos, tenantID := range tenantIDs {
			q, err := exemplarQueryable.ExemplarQuerier(user.InjectOrgID(ctx, tenantID))
			if err != nil {
				return nil, nil, err
			}
			queriers[pos] = q
		}

		return tenantIDs, queriers, nil
	}
}

// MergeExemplarQuerierCallback returns the underlying exemplar queriers and their
// IDs relevant for the query.
type MergeExemplarQuerierCallback func(ctx context.Context) (ids []string, queriers []storage.ExemplarQuerier, err error)

// NewMergeExemplarQueryable returns a queryable that merges results from multiple
// underlying ExemplarQueryables.
// By setting byPassWithSingleQuerier to true the mergeExemplarQuerier gets by-passed
// and results for request with a single exemplar querier will not contain the
// "__tenant_id__" label. This allows a smoother transition, when enabling
// tenant federation in a cluster.
// Results contain a label `idLabelName` to identify the underlying exemplar queryable
// that it originally resulted from.
// If the label `idLabelName` is already existing, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively.
func NewMergeExemplarQueryable(idLabelName string, maxConcurrent int, callback MergeExemplarQuerierCallback, byPassWithSingleQuerier bool, reg prometheus.Registerer) storage.ExemplarQueryable {
	return &mergeExemplarQueryable{
		idLabelName:             idLabelName,
		byPassWithSingleQuerier: byPassWithSingleQuerier,
		callback:                callback,
		maxConcurrent:           maxConcurrent,

		tenantsPerExemplarQuery: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "querier_federated_tenants_per_exemplar_query",
			Help:      "Number of tenants per exemplar query.",
			Buckets:   []float64{1, 2, 4, 8, 16, 32, 64},
		}),
	}
}

type mergeExemplarQueryable struct {
	idLabelName             string
	maxConcurrent           int
	byPassWithSingleQuerier bool
	callback                MergeExemplarQuerierCallback
	tenantsPerExemplarQuery prometheus.Histogram
}

// ExemplarQuerier returns a new mergeExemplarQuerier which aggregates results from
// multiple exemplar queriers into a single result.
func (m *mergeExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	ids, queriers, err := m.callback(ctx)
	if err != nil {
		return nil, err
	}

	m.tenantsPerExemplarQuery.Observe(float64(len(ids)))

	if m.byPassWithSingleQuerier && len(queriers) == 1 {
		return queriers[0], nil
	}

	return &mergeExemplarQuerier{
		ctx:                     ctx,
		idLabelName:             m.idLabelName,
		maxConcurrent:           m.maxConcurrent,
		tenantIds:               ids,
		queriers:                queriers,
		byPassWithSingleQuerier: m.byPassWithSingleQuerier,
	}, nil
}

// mergeExemplarQuerier aggregates the results from underlying exemplar queriers
// and adds a label `idLabelName` to identify the exemplar queryable that
// `seriesLabels` resulted from.
// If the label `idLabelName` is already existing, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively.
type mergeExemplarQuerier struct {
	ctx                     context.Context
	idLabelName             string
	maxConcurrent           int
	tenantIds               []string
	queriers                []storage.ExemplarQuerier
	byPassWithSingleQuerier bool
}

type exemplarSelectJob struct {
	pos     int
	querier storage.ExemplarQuerier
	id      string
}

// Select returns aggregated exemplars within given time range for multiple tenants.
func (m mergeExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	log, ctx := spanlogger.New(m.ctx, "mergeExemplarQuerier.Select")
	defer log.Span.Finish()

	// filter out tenants to query and unrelated matchers
	allMatchedTenantIds, allUnrelatedMatchers := filterAllTenantsAndMatchers(m.idLabelName, m.tenantIds, matchers)
	jobs := make([]interface{}, len(allMatchedTenantIds))
	results := make([][]exemplar.QueryResult, len(allMatchedTenantIds))

	var jobPos int
	for idx, tenantId := range m.tenantIds {
		if _, ok := allMatchedTenantIds[tenantId]; !ok {
			// skip tenantIds that should not be queried
			continue
		}

		jobs[jobPos] = &exemplarSelectJob{
			pos:     jobPos,
			querier: m.queriers[idx],
			id:      tenantId,
		}
		jobPos++
	}

	run := func(ctx context.Context, jobIntf interface{}) error {
		job, ok := jobIntf.(*exemplarSelectJob)
		if !ok {
			return fmt.Errorf("unexpected type %T", jobIntf)
		}

		res, err := job.querier.Select(start, end, allUnrelatedMatchers...)
		if err != nil {
			return errors.Wrapf(err, "error exemplars querying %s %s", rewriteLabelName(m.idLabelName), job.id)
		}

		// append __tenant__ label to `seriesLabels` to identify each tenants
		for i, e := range res {
			e.SeriesLabels = setLabelsRetainExisting(e.SeriesLabels, labels.Label{
				Name:  m.idLabelName,
				Value: job.id,
			})
			res[i] = e
		}

		results[job.pos] = res
		return nil
	}

	err := concurrency.ForEach(ctx, jobs, m.maxConcurrent, run)
	if err != nil {
		return nil, err
	}

	var ret []exemplar.QueryResult
	for _, exemplars := range results {
		ret = append(ret, exemplars...)
	}

	return ret, nil
}

func filterAllTenantsAndMatchers(idLabelName string, tenantIds []string, allMatchers [][]*labels.Matcher) (map[string]struct{}, [][]*labels.Matcher) {
	allMatchedTenantIds := make(map[string]struct{})
	allUnrelatedMatchers := make([][]*labels.Matcher, len(allMatchers))

	for idx, matchers := range allMatchers {
		matchedTenantIds, unrelatedMatchers := filterValuesByMatchers(idLabelName, tenantIds, matchers...)
		for tenantId := range matchedTenantIds {
			allMatchedTenantIds[tenantId] = struct{}{}
		}
		allUnrelatedMatchers[idx] = unrelatedMatchers
	}

	return allMatchedTenantIds, allUnrelatedMatchers
}
