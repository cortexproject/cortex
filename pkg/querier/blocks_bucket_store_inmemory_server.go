package querier

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// bucketStoreSeriesServer is an fake in-memory gRPC server used to
// call Thanos BucketStore.Series() without having to go through the
// gRPC networking stack.
type bucketStoreSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer

	ctx context.Context

	SeriesSet []*storepb.Series
	Warnings  storage.Warnings
}

func newBucketStoreSeriesServer(ctx context.Context) *bucketStoreSeriesServer {
	return &bucketStoreSeriesServer{ctx: ctx}
}

func (s *bucketStoreSeriesServer) Send(r *storepb.SeriesResponse) error {
	if r.GetWarning() != "" {
		s.Warnings = append(s.Warnings, errors.New(r.GetWarning()))
	}

	if r.GetSeries() != nil {
		s.SeriesSet = append(s.SeriesSet, r.GetSeries())
	}

	return nil
}

func (s *bucketStoreSeriesServer) Context() context.Context {
	return s.ctx
}
