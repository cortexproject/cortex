package ingester

import (
	"context"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

var (
	rangeHeadULID = ulid.MustParse("0000000000XXXXXXXRANGEHEAD")
	headULID      = ulid.MustParse("0000000000XXXXXXXXXXXXHEAD")
)

// isHead returns true if the given BlockReader is a head block (in-order or OOO).
func isHead(b tsdb.BlockReader) bool {
	id := b.Meta().ULID
	return id == rangeHeadULID || id == headULID
}

// headQueriedSeriesChunkQuerier wraps a ChunkQuerier for the head block and
// intercepts Select calls to collect series hashes for HLL tracking.
type headQueriedSeriesChunkQuerier struct {
	storage.ChunkQuerier
	headQueriedSeries          *ActiveQueriedSeries
	activeQueriedSeriesService *ActiveQueriedSeriesService
	userID                     string
	sampled                    bool
}

func (q *headQueriedSeriesChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	ss := q.ChunkQuerier.Select(ctx, sortSeries, hints, matchers...)

	// Wrap series set for hash collection only if sampled.
	if !q.sampled {
		return ss
	}
	return &headQueriedSeriesSet{
		ChunkSeriesSet:             ss,
		headQueriedSeries:          q.headQueriedSeries,
		activeQueriedSeriesService: q.activeQueriedSeriesService,
		userID:                     q.userID,
		hashes:                     getQueriedSeriesHashesSlice(),
	}
}

// headQueriedSeriesSet wraps a ChunkSeriesSet to collect series label hashes
// during iteration and flush them to the HLL tracker when iteration completes.
type headQueriedSeriesSet struct {
	storage.ChunkSeriesSet
	headQueriedSeries          *ActiveQueriedSeries
	activeQueriedSeriesService *ActiveQueriedSeriesService
	userID                     string
	hashes                     []uint64
}

func (s *headQueriedSeriesSet) Next() bool {
	if !s.ChunkSeriesSet.Next() {
		s.flush()
		return false
	}
	s.hashes = append(s.hashes, s.ChunkSeriesSet.At().Labels().Hash())
	return true
}

func (s *headQueriedSeriesSet) At() storage.ChunkSeries {
	return s.ChunkSeriesSet.At()
}

func (s *headQueriedSeriesSet) flush() {
	if len(s.hashes) > 0 && s.activeQueriedSeriesService != nil {
		s.activeQueriedSeriesService.UpdateSeriesBatch(s.headQueriedSeries, s.hashes, time.Now(), s.userID)
	} else if len(s.hashes) > 0 {
		// If no service, return the slice to the pool.
		putQueriedSeriesHashesSlice(s.hashes)
	}
}
