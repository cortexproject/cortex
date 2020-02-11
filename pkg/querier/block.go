package querier

import (
	"context"
	"fmt"
	"io"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/logging"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// BlockQuerier is a querier of thanos blocks
type BlockQuerier struct {
	us *UserStore
}

// NewBlockQuerier returns a client to query a block store
func NewBlockQuerier(cfg tsdb.Config, logLevel logging.Level, registerer prometheus.Registerer) (*BlockQuerier, error) {
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

	b := &BlockQuerier{
		us: us,
	}

	return b, nil
}

// Get implements the ChunkStore interface. It makes a block query and converts the response into chunks
func (b *BlockQuerier) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	log, ctx := spanlogger.New(ctx, "BlockQuerier.Get")
	defer log.Span.Finish()

	client := b.us.client

	// Convert matchers to LabelMatcher
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

	ctx = metadata.AppendToOutgoingContext(ctx, "user", userID)
	seriesClient, err := client.Series(ctx, &storepb.SeriesRequest{
		MinTime:                 int64(from),
		MaxTime:                 int64(through),
		Matchers:                converted,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	})
	if err != nil {
		return nil, err
	}

	var chunks []chunk.Chunk
	for {
		resp, err := seriesClient.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Convert Thanos store series into Cortex chunks
		convertedChunks, err := seriesToChunks(userID, resp.GetSeries())
		if err != nil {
			level.Error(log).Log("msg", "failed converting TSDB series to Cortex chunks", "err", err)
			return nil, err
		}

		chunks = append(chunks, convertedChunks...)
	}

	return chunks, nil
}

func seriesToChunks(userID string, series *storepb.Series) ([]chunk.Chunk, error) {
	var lbls labels.Labels
	for _, label := range series.Labels {
		// We have to remove the external label set by the shipper
		if label.Name == tsdb.TenantIDExternalLabel {
			continue
		}

		lbls = append(lbls, labels.Label{
			Name:  label.Name,
			Value: label.Value,
		})
	}

	chunks := make([]chunk.Chunk, 0, len(series.Chunks))

	for _, c := range series.Chunks {
		ch := encoding.New()

		enc, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("failed to initialize chunk from XOR encoded raw data (series: %v min time: %d max time: %d)", lbls, c.MinTime, c.MaxTime))
		}

		it := enc.Iterator(nil)
		for it.Next() {
			ts, v := it.At()
			overflow, err := ch.Add(model.SamplePair{
				Timestamp: model.Time(ts),
				Value:     model.SampleValue(v),
			})
			if err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("failed adding sample to chunk (series: %v timestamp: %d value: %f)", lbls, ts, v))
			}

			if overflow != nil {
				chunks = append(chunks, chunk.NewChunk(userID, client.Fingerprint(lbls), lbls, ch, model.Time(c.MinTime), model.Time(c.MaxTime)))
				ch = overflow
			}
		}

		// Ensure the iteration has not been interrupted because of an error
		if it.Err() != nil {
			return nil, errors.Wrap(it.Err(), fmt.Sprintf("failed reading sample from encoded chunk (series: %v min time: %d max time: %d)", lbls, c.MinTime, c.MaxTime))
		}

		if ch.Len() > 0 {
			chunks = append(chunks, chunk.NewChunk(userID, client.Fingerprint(lbls), lbls, ch, model.Time(c.MinTime), model.Time(c.MaxTime)))
		}
	}

	return chunks, nil
}
