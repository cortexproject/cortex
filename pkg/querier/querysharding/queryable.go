package querysharding

import (
	"context"
	"encoding/hex"

	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

const (
	missingEmbeddedQueryMsg = "missing embedded query"
	nonEmbeddedErrMsg       = "DownstreamQuerier cannot handle a non-embedded query"
)

// DownstreamQueryable is an implementor of the Queryable interface.
type DownstreamQueryable struct {
	Req     *queryrange.Request
	Handler queryrange.Handler
}

func (q *DownstreamQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &DownstreamQuerier{ctx, q.Req, q.Handler}, nil
}

// DownstreamQuerier is a an implementor of the Querier interface.
type DownstreamQuerier struct {
	Ctx     context.Context
	Req     *queryrange.Request
	Handler queryrange.Handler
}

// Select returns a set of series that matches the given label matchers.
func (q *DownstreamQuerier) Select(
	_ *storage.SelectParams,
	matchers ...*labels.Matcher,
) (storage.SeriesSet, storage.Warnings, error) {
	var embeddedQuery string
	var isEmbedded bool
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName && matcher.Value == astmapper.EmbeddedQueryFlag {
			isEmbedded = true
		}

		if matcher.Name == astmapper.QueryLabel {
			embeddedQuery = matcher.Value
		}
	}

	if isEmbedded {
		if embeddedQuery != "" {
			return q.handleEmbeddedQuery(embeddedQuery)
		}
		return nil, nil, errors.Errorf(missingEmbeddedQueryMsg)

	}

	return nil, nil, errors.Errorf(nonEmbeddedErrMsg)
}

// handleEmbeddedQuery defers execution of an encoded query to a downstream Handler
func (q *DownstreamQuerier) handleEmbeddedQuery(encoded string) (storage.SeriesSet, storage.Warnings, error) {
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, nil, err
	}

	resp, err := q.Handler.Do(q.Ctx, ReplaceQuery(*q.Req, string(decoded)))
	if err != nil {
		return nil, nil, err
	}

	if resp.Error != "" {
		return nil, nil, errors.Errorf(resp.Error)
	}

	set, err := ResponseToSeries(resp.Data)
	return set, nil, err

}

// LabelValues returns all potential values for a label name.
func (q *DownstreamQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	return nil, nil, errors.Errorf("unimplemented")
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q *DownstreamQuerier) LabelNames() ([]string, storage.Warnings, error) {
	return nil, nil, errors.Errorf("unimplemented")
}

// Close releases the resources of the Querier.
func (q *DownstreamQuerier) Close() error {
	return nil
}

// ReplaceQuery clones a request with a new query
func ReplaceQuery(req queryrange.Request, query string) *queryrange.Request {
	req.Query = query
	return &req
}
