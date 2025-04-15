package tenantfederation

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/scrape"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// NewMetadataQuerier returns a MetadataQuerier that merges metric
// metadata for multiple tenants.
func NewMetadataQuerier(upstream querier.MetadataQuerier, maxConcurrent int, reg prometheus.Registerer) querier.MetadataQuerier {
	return &mergeMetadataQuerier{
		upstream:      upstream,
		maxConcurrent: maxConcurrent,

		tenantsPerMetadataQuery: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "querier_federated_tenants_per_metadata_query",
			Help:      "Number of tenants per metadata query.",
			Buckets:   []float64{1, 2, 4, 8, 16, 32, 64},
		}),
	}
}

type mergeMetadataQuerier struct {
	maxConcurrent           int
	tenantsPerMetadataQuery prometheus.Histogram
	upstream                querier.MetadataQuerier
}

type metadataSelectJob struct {
	pos     int
	querier querier.MetadataQuerier
	id      string
}

// MetricsMetadata returns aggregated metadata for multiple tenants
func (m *mergeMetadataQuerier) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error) {
	log, ctx := spanlogger.New(ctx, "mergeMetadataQuerier.MetricsMetadata")
	defer log.Span.Finish()

	tenantIds, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}

	m.tenantsPerMetadataQuery.Observe(float64(len(tenantIds)))

	if len(tenantIds) == 1 {
		return m.upstream.MetricsMetadata(ctx, req)
	}

	jobs := make([]interface{}, len(tenantIds))
	results := make([][]scrape.MetricMetadata, len(tenantIds))

	var jobPos int
	for _, tenantId := range tenantIds {
		jobs[jobPos] = &metadataSelectJob{
			pos:     jobPos,
			querier: m.upstream,
			id:      tenantId,
		}
		jobPos++
	}

	run := func(ctx context.Context, jobIntf interface{}) error {
		job, ok := jobIntf.(*metadataSelectJob)
		if !ok {
			return fmt.Errorf("unexpected type %T", jobIntf)
		}

		res, err := job.querier.MetricsMetadata(user.InjectOrgID(ctx, job.id), req)
		if err != nil {
			return errors.Wrapf(err, "error exemplars querying %s %s", job.id, err)
		}

		results[job.pos] = res
		return nil
	}

	err = concurrency.ForEach(ctx, jobs, m.maxConcurrent, run)
	if err != nil {
		return nil, err
	}

	// deduplicate for the same MetricMetadata across all tenants
	var ret []scrape.MetricMetadata
	deduplicated := make(map[scrape.MetricMetadata]struct{})
	for _, metadata := range results {
		for _, m := range metadata {
			if _, ok := deduplicated[m]; !ok {
				ret = append(ret, m)
				deduplicated[m] = struct{}{}
			}
		}
	}

	return ret, nil
}
