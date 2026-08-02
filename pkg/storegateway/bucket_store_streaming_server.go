package storegateway

import (
	"context"
	"sync"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// channelSeriesServer adapts a push-based storepb.Store_SeriesServer into a pull-based
// storepb.SeriesSet backed by a bounded channel. A producer goroutine runs a sub-store's
// Series() with this server, and Send pushes each received series onto the channel. The
// consumer pulls them via Next/At/Err so storepb.MergeSeriesSets can stream-merge multiple
// stores without buffering full results.
type channelSeriesServer struct {
	storepb.Store_SeriesServer

	ctx context.Context
	ch  chan *storepb.Series

	warnings annotations.Annotations
	hints    hintspb.SeriesResponseHints

	mu  sync.Mutex
	err error

	// cur holds the series returned by the most recent successful Next call.
	cur *storepb.Series
}

func newChannelSeriesServer(ctx context.Context, bufferSize int) *channelSeriesServer {
	if bufferSize < 1 {
		bufferSize = 1
	}
	return &channelSeriesServer{
		ctx: ctx,
		ch:  make(chan *storepb.Series, bufferSize),
	}
}

// Context implements storepb.Store_SeriesServer.
func (s *channelSeriesServer) Context() context.Context { return s.ctx }

// Send implements storepb.Store_SeriesServer. It handles single Series, batched Series, warning
// and hints responses.
func (s *channelSeriesServer) Send(r *storepb.SeriesResponse) error {
	if w := r.GetWarning(); w != "" {
		s.warnings.Add(errors.New(w))
	}

	if rawHints := r.GetHints(); rawHints != nil {
		if err := types.UnmarshalAny(rawHints, &s.hints); err != nil {
			return errors.Wrap(err, "failed to unmarshal series hints")
		}
	}

	if series := r.GetSeries(); series != nil {
		return s.push(series)
	}

	if batch := r.GetBatch(); batch != nil {
		for _, series := range batch.Series {
			if err := s.push(series); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *channelSeriesServer) push(series *storepb.Series) error {
	// Thanos uses pools for the chunks and may use other pools in the future. Given we need to
	// retain the reference after the pooled slices are recycled, we copy via marshal+unmarshal.
	data, err := series.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshal received series")
	}
	copied := &storepb.Series{}
	if err := copied.Unmarshal(data); err != nil {
		return errors.Wrap(err, "unmarshal received series")
	}

	select {
	case s.ch <- copied:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// Close records the producer's terminal error (if any) and closes the channel. It must be called
// exactly once by the producer goroutine after Series() returns.
func (s *channelSeriesServer) Close(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	close(s.ch)
}

// Next implements storepb.SeriesSet.
func (s *channelSeriesServer) Next() bool {
	series, ok := <-s.ch
	if !ok {
		return false
	}
	s.cur = series
	return true
}

// At implements storepb.SeriesSet.
func (s *channelSeriesServer) At() (labels.Labels, []storepb.AggrChunk) {
	return s.cur.PromLabels(), s.cur.Chunks
}

// Err implements storepb.SeriesSet. It is safe to call concurrently while the producer is running.
func (s *channelSeriesServer) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}
