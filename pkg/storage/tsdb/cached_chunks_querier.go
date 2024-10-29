package tsdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/util/annotations"
)

type PostingsCacheConfig struct {
	MaxBytes int64         `yaml:"max_bytes"`
	MaxItems int           `yaml:"max_items"`
	Ttl      time.Duration `yaml:"ttl"`
	Enabled  bool          `yaml:"enabled"`
}

type blockBaseQuerier struct {
	blockID    ulid.ULID
	index      prom_tsdb.IndexReader
	chunks     prom_tsdb.ChunkReader
	tombstones tombstones.Reader

	closed bool

	mint, maxt int64
}

func newBlockBaseQuerier(b prom_tsdb.BlockReader, mint, maxt int64) (*blockBaseQuerier, error) {
	indexr, err := b.Index()
	if err != nil {
		return nil, fmt.Errorf("open index reader: %w", err)
	}
	chunkr, err := b.Chunks()
	if err != nil {
		indexr.Close()
		return nil, fmt.Errorf("open chunk reader: %w", err)
	}
	tombsr, err := b.Tombstones()
	if err != nil {
		indexr.Close()
		chunkr.Close()
		return nil, fmt.Errorf("open tombstone reader: %w", err)
	}

	if tombsr == nil {
		tombsr = tombstones.NewMemTombstones()
	}
	return &blockBaseQuerier{
		blockID:    b.Meta().ULID,
		mint:       mint,
		maxt:       maxt,
		index:      indexr,
		chunks:     chunkr,
		tombstones: tombsr,
	}, nil
}

func (q *blockBaseQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	res, err := q.index.SortedLabelValues(ctx, name, matchers...)
	return res, nil, err
}

func (q *blockBaseQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	res, err := q.index.LabelNames(ctx, matchers...)
	return res, nil, err
}

func (q *blockBaseQuerier) Close() error {
	if q.closed {
		return errors.New("block querier already closed")
	}

	errs := tsdb_errors.NewMulti(
		q.index.Close(),
		q.chunks.Close(),
		q.tombstones.Close(),
	)
	q.closed = true
	return errs.Err()
}

type cachedBlockChunkQuerier struct {
	*blockBaseQuerier
}

func NewCachedBlockChunkQuerier(cfg PostingsCacheConfig, b prom_tsdb.BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := newBlockBaseQuerier(b, mint, maxt)
	if err != nil {
		return nil, err
	}
	return &cachedBlockChunkQuerier{blockBaseQuerier: q}, nil
}

func (q *cachedBlockChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.ChunkSeriesSet {
	return selectChunkSeriesSet(ctx, sortSeries, hints, ms, q.blockID, q.index, q.chunks, q.tombstones, q.mint, q.maxt)
}

func selectChunkSeriesSet(ctx context.Context, sortSeries bool, hints *storage.SelectHints, ms []*labels.Matcher,
	blockID ulid.ULID, index prom_tsdb.IndexReader, chunks prom_tsdb.ChunkReader, tombstones tombstones.Reader, mint, maxt int64,
) storage.ChunkSeriesSet {
	disableTrimming := false
	sharded := hints != nil && hints.ShardCount > 0

	if hints != nil {
		mint = hints.Start
		maxt = hints.End
		disableTrimming = hints.DisableTrimming
	}
	p, err := prom_tsdb.PostingsForMatchers(ctx, index, ms...)
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}
	if sharded {
		p = index.ShardedPostings(p, hints.ShardIndex, hints.ShardCount)
	}
	if sortSeries {
		p = index.SortedPostings(p)
	}

	if hints != nil {
		if hints.Func == "series" {
			// When you're only looking up metadata (for example series API), you don't need to load any chunks.
			return prom_tsdb.NewBlockChunkSeriesSet(blockID, index, NewNopChunkReader(), tombstones, p, mint, maxt, disableTrimming)
		}
	}

	return prom_tsdb.NewBlockChunkSeriesSet(blockID, index, chunks, tombstones, p, mint, maxt, disableTrimming)
}

type nopChunkReader struct {
	emptyChunk chunkenc.Chunk
}

func NewNopChunkReader() prom_tsdb.ChunkReader {
	return nopChunkReader{
		emptyChunk: chunkenc.NewXORChunk(),
	}
}

func (cr nopChunkReader) ChunkOrIterable(chunks.Meta) (chunkenc.Chunk, chunkenc.Iterable, error) {
	return cr.emptyChunk, nil, nil
}

func (cr nopChunkReader) Close() error { return nil }
