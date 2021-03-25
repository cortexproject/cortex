package builder

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/querier/iterators"
)

const (
	unsortedChunksDir     = "unsorted_chunks"
	readChunksConcurrency = 16
)

// This builder uses TSDB's chunk and index writer directly, without
// using TSDB Head.
type tsdbBuilder struct {
	log log.Logger

	ulid        ulid.ULID
	outDir      string
	tmpBlockDir string

	unsortedChunksWriterMu sync.Mutex
	unsortedChunksWriter   tsdb.ChunkWriter

	startTime          model.Time
	endTime            model.Time
	timestampTolerance int // in milliseconds

	series    *seriesList
	seriesDir string

	writtenSamples  prometheus.Counter
	processedSeries prometheus.Counter
	seriesInMemory  prometheus.Gauge
}

func newTsdbBuilder(outDir string, start, end time.Time, timestampTolerance time.Duration, seriesBatchLimit int, log log.Logger, processedSeries, writtenSamples prometheus.Counter, seriesInMemory prometheus.Gauge) (*tsdbBuilder, error) {
	id, err := ulid.New(ulid.Now(), rand.Reader)
	if err != nil {
		return nil, errors.Wrap(err, "create ULID")
	}

	blockDir := filepath.Join(outDir, id.String()+".tmp")
	seriesDir := filepath.Join(blockDir, "series")

	err = os.RemoveAll(blockDir)
	if err != nil {
		return nil, err
	}

	// Also makes blockDir, if missing
	err = os.MkdirAll(seriesDir, 0777)
	if err != nil {
		return nil, err
	}

	unsortedChunksWriter, err := chunks.NewWriter(filepath.Join(blockDir, unsortedChunksDir))
	if err != nil {
		return nil, errors.Wrap(err, "chunks writer")
	}

	return &tsdbBuilder{
		log:                  log,
		ulid:                 id,
		outDir:               outDir,
		tmpBlockDir:          blockDir,
		unsortedChunksWriter: unsortedChunksWriter,
		startTime:            model.TimeFromUnixNano(start.UnixNano()),
		endTime:              model.TimeFromUnixNano(end.UnixNano()),
		timestampTolerance:   int(timestampTolerance.Milliseconds()),
		series:               newSeriesList(seriesBatchLimit, seriesDir),
		seriesDir:            seriesDir,

		processedSeries: processedSeries,
		writtenSamples:  writtenSamples,
		seriesInMemory:  seriesInMemory,
	}, err
}

// Called concurrently with all chunks required to build a single series.
func (d *tsdbBuilder) buildSingleSeries(metric labels.Labels, cs []chunk.Chunk) error {
	defer d.processedSeries.Inc()

	// Used by Prometheus, in head.go (with a reference to Gorilla paper).
	const samplesPerChunk = 120

	chs := make([]chunks.Meta, 0, 25) // On average, single series seem to have around 25 chunks.
	seriesSamples := uint64(0)

	// current chunk and appender. If nil, new chunk will be created.
	var (
		ch  *chunks.Meta
		app chunkenc.Appender
		err error
	)

	// This will merge and deduplicate samples from chunks.
	it := iterators.NewChunkMergeIterator(cs, d.startTime, d.endTime)
	for it.Next() && it.Err() == nil {
		t, v := it.At()

		mt := model.Time(t)

		if mt < d.startTime {
			continue
		}
		if mt >= d.endTime {
			break
		}

		if ch == nil {
			chs = append(chs, chunks.Meta{})
			ch = &chs[len(chs)-1]
			ch.MinTime = t

			ch.Chunk = chunkenc.NewXORChunk()
			app, err = ch.Chunk.Appender()
			if err != nil {
				return err
			}
		}

		// If gap since last scrape is very close to an exact number of seconds, tighten it up
		if d.timestampTolerance != 0 && ch.MaxTime != 0 {
			gap := t - ch.MaxTime
			seconds := ((gap + 500) / 1000)
			diff := int(gap - seconds*1000)
			// Don't go past endTime.
			if diff != 0 && diff >= -d.timestampTolerance && diff <= d.timestampTolerance && ch.MaxTime+seconds*1000 <= int64(d.endTime) {
				t = ch.MaxTime + seconds*1000
			}
		}

		ch.MaxTime = t
		app.Append(t, v)
		seriesSamples++

		if ch.Chunk.NumSamples() == samplesPerChunk {
			ch.Chunk.Compact()
			ch = nil
		}
	}

	if ch != nil {
		ch.Chunk.Compact()
		ch = nil
	}

	d.unsortedChunksWriterMu.Lock()
	err = d.unsortedChunksWriter.WriteChunks(chs...)
	d.unsortedChunksWriterMu.Unlock()

	if err != nil {
		return err
	}

	// Remove chunks data from memory, but keep reference.
	for ix := range chs {
		if chs[ix].Ref == 0 {
			return errors.Errorf("chunk ref not set")
		}
		chs[ix].Chunk = nil
	}

	// No samples, ignore.
	if len(chs) == 0 {
		return nil
	}

	minTime := chs[0].MinTime
	maxTime := chs[len(chs)-1].MaxTime

	err = d.series.addSeries(metric, chs, seriesSamples, minTime, maxTime)

	d.seriesInMemory.Set(float64(d.series.unflushedSeries()))
	d.writtenSamples.Add(float64(seriesSamples))
	return err
}

func (d *tsdbBuilder) finishBlock(source string, labels map[string]string) (ulid.ULID, error) {
	if err := d.unsortedChunksWriter.Close(); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "closing chunks writer")
	}

	if err := d.series.flushSeries(); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "flushing series")
	}
	d.seriesInMemory.Set(0)

	level.Info(d.log).Log("msg", "all chunks fetched, building block index")

	meta := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    d.ulid,
			Version: 1,
			MinTime: int64(d.startTime),
			MaxTime: int64(d.endTime),
			Compaction: tsdb.BlockMetaCompaction{
				Level:   1,
				Sources: []ulid.ULID{d.ulid},
			},
		},

		// We populate SegmentFiles (which is deprecated, but still used). The new Files property
		// will be populated by Thanos block.Upload().
		Thanos: metadata.Thanos{
			Labels:       labels,
			Source:       metadata.SourceType(source),
			SegmentFiles: block.GetSegmentFiles(d.tmpBlockDir),
		},
	}

	toClose := map[string]io.Closer{}
	defer func() {
		for k, c := range toClose {
			err := c.Close()
			if err != nil {
				level.Error(d.log).Log("msg", "close failed", "name", k, "err", err)
			}
		}
	}()

	const (
		indexWriterName          = "index writer"
		unsortedChunksReaderName = "unsorted chunks reader"
		chunksWriterName         = "chunks writer"
	)

	indexWriter, err := index.NewWriter(context.Background(), filepath.Join(d.tmpBlockDir, "index"))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, indexWriterName)
	}
	toClose[indexWriterName] = indexWriter

	symbols, err := addSymbolsToIndex(indexWriter, d.series)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "adding symbols")
	}

	level.Info(d.log).Log("msg", "added symbols to index", "count", symbols)

	unsortedChunksReader, err := chunks.NewDirReader(filepath.Join(d.tmpBlockDir, unsortedChunksDir), nil)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, unsortedChunksReaderName)
	}
	toClose[unsortedChunksReaderName] = unsortedChunksReader

	chunksWriter, err := chunks.NewWriter(filepath.Join(d.tmpBlockDir, "chunks"))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, chunksWriterName)
	}
	toClose[chunksWriterName] = chunksWriter

	stats, err := addSeriesToIndex(indexWriter, d.series, unsortedChunksReader, chunksWriter)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "adding series")
	}
	meta.Stats = stats

	level.Info(d.log).Log("msg", "added series to index", "series", stats.NumSeries, "chunks", stats.NumChunks, "samples", stats.NumSamples)

	// Close index writer, unsorted chunks reader and chunks writer.
	for k, c := range toClose {
		delete(toClose, k)

		err := c.Close()
		if err != nil {
			return ulid.ULID{}, errors.Wrapf(err, "closing %s", k)
		}
	}

	// Delete unsorted chunks, they are no longer needed.
	if err := os.RemoveAll(filepath.Join(d.tmpBlockDir, unsortedChunksDir)); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "deleting unsorted chunks")
	}

	if err := meta.WriteToDir(d.log, d.tmpBlockDir); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "writing meta.json")
	}

	if err := os.Rename(d.tmpBlockDir, filepath.Join(d.outDir, d.ulid.String())); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "rename to final directory")
	}

	return d.ulid, nil
}

func addSeriesToIndex(indexWriter *index.Writer, sl *seriesList, unsortedChunksReader *chunks.Reader, chunksWriter *chunks.Writer) (tsdb.BlockStats, error) {
	var stats tsdb.BlockStats

	it, err := sl.seriesIterator()
	if err != nil {
		return stats, errors.Wrap(err, "reading series")
	}

	type chunkToRead struct {
		ref   uint64
		chunk *chunkenc.Chunk
		err   *error
	}

	ch := make(chan chunkToRead)
	defer close(ch) // To make sure that goroutines stop.

	// Number of chunks that should be loaded.
	var pendingChunks sync.WaitGroup

	// These goroutines read chunks into memory.
	for n := 0; n < readChunksConcurrency; n++ {
		go func() {
			for ctr := range ch {
				c, e := unsortedChunksReader.Chunk(ctr.ref)
				*ctr.chunk = c
				*ctr.err = e
				pendingChunks.Done()
			}
		}()
	}

	seriesRef := 0
	for s, ok := it.Next(); ok; s, ok = it.Next() {
		l := s.Metric
		cs := s.Chunks

		readErrors := make([]error, len(cs))

		// Read chunks into memory by asking goroutines to load them.
		for ix := range cs {
			pendingChunks.Add(1)
			ch <- chunkToRead{
				ref:   cs[ix].Ref,
				chunk: &cs[ix].Chunk,
				err:   &readErrors[ix],
			}
			cs[ix].Ref = 0
		}

		// Wait for all chunks to be fetched.
		pendingChunks.Wait()

		multi := tsdb_errors.NewMulti()
		for _, e := range readErrors {
			if e != nil {
				multi.Add(e)
			}
		}
		if err := multi.Err(); err != nil {
			return stats, errors.Wrap(err, "failed to read chunks")
		}

		// Write chunks again. This time they will be written in the same order as series.
		err = chunksWriter.WriteChunks(cs...)
		if err != nil {
			return stats, errors.Wrap(err, "failed to write sorted chunks")
		}

		// Remove chunks data from memory, but keep reference for writing to index.
		for ix := range cs {
			if cs[ix].Ref == 0 {
				return stats, errors.Errorf("chunk ref not set after writing sorted chunks")
			}
			cs[ix].Chunk = nil
		}

		if err := indexWriter.AddSeries(uint64(seriesRef), l, cs...); err != nil {
			return stats, errors.Wrapf(err, "adding series %v", l)
		}

		seriesRef++

		stats.NumSamples += s.Samples
		stats.NumSeries++
		stats.NumChunks += uint64(len(cs))
	}

	return stats, nil
}

func addSymbolsToIndex(indexWriter *index.Writer, sl *seriesList) (int, error) {
	symbols := 0
	it, err := sl.symbolsIterator()
	if err != nil {
		return 0, errors.Wrap(err, "reading symbols")
	}

	for s, ok := it.Next(); ok; s, ok = it.Next() {
		symbols++
		if err := indexWriter.AddSymbol(s); err != nil {
			_ = it.Close() // Make sure to close any open files.
			return 0, errors.Wrapf(err, "adding symbol %v", s)
		}
	}
	if err := it.Error(); err != nil {
		_ = it.Close() // Make sure to close any open files.
		return 0, err
	}

	if err := it.Close(); err != nil {
		return 0, err
	}

	return symbols, nil
}
