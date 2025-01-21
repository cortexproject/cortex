package compactor

import (
	"context"
	"io"
	"maps"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type ShardedBlockPopulator struct {
	partitionCount int
	partitionID    int
	logger         log.Logger
}

// PopulateBlock fills the index and chunk writers with new data gathered as the union
// of the provided blocks. It returns meta information for the new block.
// It expects sorted blocks input by mint.
// The main logic is copied from tsdb.DefaultPopulateBlockFunc
func (c ShardedBlockPopulator) PopulateBlock(ctx context.Context, metrics *tsdb.CompactorMetrics, _ log.Logger, chunkPool chunkenc.Pool, mergeFunc storage.VerticalChunkSeriesMergeFunc, blocks []tsdb.BlockReader, meta *tsdb.BlockMeta, indexw tsdb.IndexWriter, chunkw tsdb.ChunkWriter, postingsFunc tsdb.IndexReaderPostingsFunc) (err error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}

	var (
		sets    []storage.ChunkSeriesSet
		setsMtx sync.Mutex
		symbols map[string]struct{}
		closers []io.Closer
	)
	symbols = make(map[string]struct{})
	defer func() {
		errs := tsdb_errors.NewMulti(err)
		if cerr := tsdb_errors.CloseAll(closers); cerr != nil {
			errs.Add(errors.Wrap(cerr, "close"))
		}
		err = errs.Err()
		metrics.PopulatingBlocks.Set(0)
	}()
	metrics.PopulatingBlocks.Set(1)

	globalMaxt := blocks[0].Meta().MaxTime
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, b := range blocks {
		select {
		case <-gCtx.Done():
			return gCtx.Err()
		default:
		}

		if b.Meta().MaxTime > globalMaxt {
			globalMaxt = b.Meta().MaxTime
		}

		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %+v", b.Meta())
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %+v", b.Meta())
		}
		closers = append(closers, chunkr)

		tombsr, err := b.Tombstones()
		if err != nil {
			return errors.Wrapf(err, "open tombstone reader for block %+v", b.Meta())
		}
		closers = append(closers, tombsr)

		all := postingsFunc(gCtx, indexr)
		g.Go(func() error {
			shardStart := time.Now()
			shardedPosting, syms, err := NewShardedPosting(gCtx, all, uint64(c.partitionCount), uint64(c.partitionID), indexr.Series)
			if err != nil {
				return err
			}
			level.Debug(c.logger).Log("msg", "finished sharding", "duration", time.Since(shardStart))
			// Blocks meta is half open: [min, max), so subtract 1 to ensure we don't hold samples with exact meta.MaxTime timestamp.
			setsMtx.Lock()
			sets = append(sets, tsdb.NewBlockChunkSeriesSet(meta.ULID, indexr, chunkr, tombsr, shardedPosting, meta.MinTime, meta.MaxTime-1, false))
			maps.Copy(symbols, syms)
			setsMtx.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	symbolsList := make([]string, len(symbols))
	symbolIdx := 0
	for symbol := range symbols {
		symbolsList[symbolIdx] = symbol
		symbolIdx++
	}
	slices.Sort(symbolsList)
	for _, symbol := range symbolsList {
		if err := indexw.AddSymbol(symbol); err != nil {
			return errors.Wrap(err, "add symbol")
		}
	}

	var (
		ref = storage.SeriesRef(0)
		ch  = make(chan func() error, 1000)
	)

	set := sets[0]
	if len(sets) > 1 {
		iCtx, cancel := context.WithCancel(ctx)
		// Merge series using specified chunk series merger.
		// The default one is the compacting series merger.
		set = NewBackgroundChunkSeriesSet(iCtx, storage.NewMergeChunkSeriesSet(sets, mergeFunc))
		defer cancel()
	}

	go func() {
		// Iterate over all sorted chunk series.
		for set.Next() {
			select {
			case <-ctx.Done():
				ch <- func() error { return ctx.Err() }
			default:
			}
			s := set.At()
			curChksIter := s.Iterator(nil)

			var chks []chunks.Meta
			var wg sync.WaitGroup
			r := ref
			wg.Add(1)
			go func() {
				for curChksIter.Next() {
					// We are not iterating in streaming way over chunk as
					// it's more efficient to do bulk write for index and
					// chunk file purposes.
					chks = append(chks, curChksIter.At())
				}
				wg.Done()
			}()

			ch <- func() error {
				wg.Wait()
				if curChksIter.Err() != nil {
					return errors.Wrap(curChksIter.Err(), "chunk iter")
				}

				// Skip the series with all deleted chunks.
				if len(chks) == 0 {
					return nil
				}

				if err := chunkw.WriteChunks(chks...); err != nil {
					return errors.Wrap(err, "write chunks")
				}
				if err := indexw.AddSeries(r, s.Labels(), chks...); err != nil {
					return errors.Wrap(err, "add series")
				}

				meta.Stats.NumChunks += uint64(len(chks))
				meta.Stats.NumSeries++
				for _, chk := range chks {
					meta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
				}

				for _, chk := range chks {
					if err := chunkPool.Put(chk.Chunk); err != nil {
						return errors.Wrap(err, "put chunk")
					}
				}

				return nil
			}

			ref++
		}
		close(ch)
	}()

	for callback := range ch {
		err := callback()
		if err != nil {
			return err
		}
	}

	if set.Err() != nil {
		return errors.Wrap(set.Err(), "iterate compaction set")
	}

	return nil
}
