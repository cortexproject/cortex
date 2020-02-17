package querier

import (
	"context"
	"io"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// BlockQueryable is a storage.Queryable implementation for blocks storage
type BlockQueryable struct {
	us *UserStore
}

// NewBlockQueryable returns a client to query a block store
func NewBlockQueryable(cfg tsdb.Config, logLevel logging.Level, registerer prometheus.Registerer) (*BlockQueryable, error) {
	bucketClient, err := tsdb.NewBucketClient(context.Background(), cfg, "cortex-userstore", util.Logger)
	if err != nil {
		return nil, err
	}

	if registerer != nil {
		bucketClient = objstore.BucketWithMetrics( /* bucket label value */ "", bucketClient, prometheus.WrapRegistererWithPrefix("cortex_querier_", registerer))
	}

	us, err := NewUserStore(cfg, bucketClient, logLevel, util.Logger, registerer)
	if err != nil {
		return nil, err
	}

	b := &BlockQueryable{us: us}

	return b, nil
}

// Querier returns a new Querier on the storage.
func (b *BlockQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, promql.ErrStorage{Err: err}
	}

	return &blocksQuerier{
		ctx:    ctx,
		client: b.us.client,
		mint:   mint,
		maxt:   maxt,
		userID: userID,
	}, nil
}

type blocksQuerier struct {
	ctx        context.Context
	client     storepb.StoreClient
	mint, maxt int64
	userID     string
}

func (b *blocksQuerier) addUserToContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "user", b.userID)
}

func (b *blocksQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	log, ctx := spanlogger.New(b.ctx, "blocksQuerier.Select")
	defer log.Span.Finish()

	mint, maxt := b.mint, b.maxt
	if sp != nil {
		mint, maxt = sp.Start, sp.End
	}
	converted := convertMatchersToLabelMatcher(matchers)

	ctx = b.addUserToContext(ctx)
	// returned series are sorted
	seriesClient, err := b.client.Series(ctx, &storepb.SeriesRequest{
		MinTime:                 mint,
		MaxTime:                 maxt,
		Matchers:                converted,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	})
	if err != nil {
		return nil, nil, err
	}

	return &blockQuerierSeriesSet{
		seriesClient: seriesClient,
	}, nil, nil
}

func convertMatchersToLabelMatcher(matchers []*labels.Matcher) []storepb.LabelMatcher {
	var converted []storepb.LabelMatcher
	for _, m := range matchers {
		var t storepb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			t = storepb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = storepb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = storepb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = storepb.LabelMatcher_NRE
		}

		converted = append(converted, storepb.LabelMatcher{
			Type:  t,
			Name:  m.Name,
			Value: m.Value,
		})
	}
	return converted
}

func (b *blocksQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	log, ctx := spanlogger.New(b.ctx, "blocksQuerier.LabelValues")
	defer log.Span.Finish()

	ctx = b.addUserToContext(ctx)
	resp, err := b.client.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label:                   name,
		PartialResponseDisabled: false,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	})
	if err != nil {
		return nil, nil, err
	}

	return resp.Values, nil, nil
}

func (b *blocksQuerier) LabelNames() ([]string, storage.Warnings, error) {
	log, ctx := spanlogger.New(b.ctx, "blocksQuerier.LabelValues")
	defer log.Span.Finish()

	ctx = b.addUserToContext(ctx)
	resp, err := b.client.LabelNames(ctx, &storepb.LabelNamesRequest{
		PartialResponseDisabled: false,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	})
	if err != nil {
		return nil, nil, err
	}

	return resp.Names, nil, nil
}

func (b *blocksQuerier) Close() error {
	// nothing to do here.
	return nil
}

// This is super-interface of storepb.Store_SeriesClient
// blockQuerierSeriesSet only uses this single method, so to simplify tests, we
// use this simplified interface.
type Store_SeriesClient interface {
	Recv() (*storepb.SeriesResponse, error)
}

// implementation of storage.SeriesSet, based streamed responses from store client.
type blockQuerierSeriesSet struct {
	seriesClient Store_SeriesClient

	err error

	currSeries *storepb.Series
	currChunks []storepb.AggrChunk

	nextSeries *storepb.Series
}

func (bqss *blockQuerierSeriesSet) Next() bool {
	if bqss.err != nil {
		return false
	}

	bqss.currSeries, bqss.currChunks = nil, nil

	// if there was any cached 'next' series, let's use it.
	if bqss.nextSeries != nil {
		bqss.currSeries = bqss.nextSeries
		bqss.currChunks = append(bqss.currChunks, bqss.currSeries.Chunks...)
		bqss.nextSeries = nil
	}

	// collect chunks for current series. Chunks may come in multiple responses, but as soon
	// as the response has chunks for a new series, we can stop searching. Series are sorted.
	// See documentation for StoreClient.Series call for details.
	for {
		r, err := bqss.seriesClient.Recv()
		if err != nil {
			bqss.err = err
			break
		}

		s := r.GetSeries()
		// response may either contain series or warning. If it's warning, we get nil here. We ignore warnings, as it's
		// too late to return them.
		if s == nil {
			continue
		}

		// check if it's still the same series as the 'current' one
		if bqss.currSeries == nil {
			bqss.currSeries = s
			bqss.currChunks = append(bqss.currChunks, s.Chunks...)
			// Scan for more chunks in subsequent responses
		} else if storepb.CompareLabels(bqss.currSeries.Labels, s.Labels) == 0 {
			bqss.currChunks = append(bqss.currChunks, s.Chunks...)
		} else {
			// We have received chunks for next series. Keep it for later, and
			// stop collecting chunks for the current one.
			bqss.nextSeries = s
			break
		}
	}

	if bqss.err != nil && bqss.err != io.EOF {
		return false
	}

	return bqss.currSeries != nil && len(bqss.currChunks) > 0
}

func (bqss *blockQuerierSeriesSet) At() storage.Series {
	if bqss.currSeries == nil {
		return nil
	}

	return newBlockQuerierSeries(bqss.currSeries.Labels, bqss.currChunks)
}

func (bqss *blockQuerierSeriesSet) Err() error {
	if bqss.err != nil && bqss.err != io.EOF {
		return bqss.err
	}

	return nil
}

func newBlockQuerierSeries(lbls []storepb.Label, chunks []storepb.AggrChunk) *blockQuerierSeries {
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].MinTime < chunks[j].MinTime
	})

	// precompute labels
	modLabels := make(labels.Labels, 0, len(lbls))
	for _, l := range lbls {
		// We have to remove the external label set by the shipper
		if l.Name == tsdb.TenantIDExternalLabel {
			continue
		}
		modLabels = append(modLabels, labels.Label{Name: l.Name, Value: l.Value})
	}

	// sort labels to make sure we obey labels.Labels contract.
	sort.Slice(modLabels, func(i, j int) bool {
		return strings.Compare(modLabels[i].Name, modLabels[j].Name) < 0
	})

	return &blockQuerierSeries{labels: modLabels, chunks: chunks}
}

type blockQuerierSeries struct {
	labels labels.Labels
	chunks []storepb.AggrChunk
}

func (bqs *blockQuerierSeries) Labels() labels.Labels {
	return bqs.labels
}

func (bqs *blockQuerierSeries) Iterator() storage.SeriesIterator {
	if len(bqs.chunks) == 0 {
		// should not happen in practice, but we have a unit test for it
		return errIterator{err: errors.New("no chunks")}
	}

	its := make([]chunkenc.Iterator, 0, len(bqs.chunks))

	for _, c := range bqs.chunks {
		ch, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		if err != nil {
			return errIterator{err: errors.Wrapf(err, "failed to initialize chunk from XOR encoded raw data (series: %v min time: %d max time: %d)", bqs.Labels(), c.MinTime, c.MaxTime)}
		}

		it := ch.Iterator(nil)
		its = append(its, it)
	}

	return &blockQuerierSeriesIterator{series: bqs, iterators: its}
}

// blockQuerierSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type blockQuerierSeriesIterator struct {
	// only used for reporting errors
	series *blockQuerierSeries

	iterators []chunkenc.Iterator
	i         int
}

func (it *blockQuerierSeriesIterator) Seek(t int64) (ok bool) {
	// We generally expect the chunks already to be cut down
	// to the range we are interested in. There's not much to be gained from
	// hopping across chunks so we just call next until we reach t.
	for {
		ct, _ := it.At()
		if ct >= t {
			return true
		}
		if !it.Next() {
			return false
		}
	}
}

func (it *blockQuerierSeriesIterator) At() (t int64, v float64) {
	return it.iterators[it.i].At()
}

func (it *blockQuerierSeriesIterator) Next() bool {
	lastT, _ := it.At()

	if it.iterators[it.i].Next() {
		return true
	}
	if it.Err() != nil {
		return false
	}
	if it.i >= len(it.iterators)-1 {
		return false
	}
	// Chunks are guaranteed to be ordered but not generally guaranteed to not overlap.
	// We must ensure to skip any overlapping range between adjacent chunks.
	it.i++
	return it.Seek(lastT + 1)
}

func (it *blockQuerierSeriesIterator) Err() error {
	err := it.iterators[it.i].Err()
	if err != nil {
		c := it.series.chunks[it.i]
		return errors.Wrapf(err, "cannot iterate chunk for series: %v min time: %d max time: %d", it.series.Labels(), c.MinTime, c.MaxTime)
	}
	return nil
}
