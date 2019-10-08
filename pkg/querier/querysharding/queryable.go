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

// DownstreamQueryable is a wrapper for and implementor of the Queryable interface.
type DownstreamQueryable struct {
	storage.Queryable
	req     *queryrange.Request
	handler queryrange.Handler
}

func (q *DownstreamQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	querier, err := q.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}

	return &downstreamQuerier{querier, ctx, q.handler, mint, maxt, q.req}, nil
}

// downstreamQuerier is a wrapper and implementor of the Querier interface
type downstreamQuerier struct {
	storage.Querier
	ctx        context.Context
	handler    queryrange.Handler
	mint, maxt int64
	req        *queryrange.Request
}

// Select returns a set of series that matches the given label matchers.
func (q *downstreamQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	for _, matcher := range matchers {
		if matcher.Name == astmapper.EMBEDDED_QUERY_FLAG {
			// this is an embedded query
			return q.handleEmbeddedQuery(matcher.Value)
		}
	}

	// otherwise pass through to wrapped querier
	// TODO: do we want to ship non-embedded selects downstream?
	return q.Querier.Select(sp, matchers...)
}

// handleEmbeddedQuery defers execution of an encoded query to a downstream handler
func (q *downstreamQuerier) handleEmbeddedQuery(encoded string) (storage.SeriesSet, storage.Warnings, error) {
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, nil, err
	}

	resp, err := q.handler.Do(q.ctx, ReplaceQuery(*q.req, string(decoded)))
	if err != nil {
		return nil, nil, err
	}

	if resp.Error != "" {
		return nil, nil, errors.Errorf(resp.Error)
	}

	set, err := ResponseToSeries(resp.Data)
	return set, nil, err

}

// take advantage of pass by value to clone a request with a new query
func ReplaceQuery(req queryrange.Request, query string) *queryrange.Request {
	req.Query = query
	return &req
}
