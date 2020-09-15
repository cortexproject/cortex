package builder

import (
	"context"
	"crypto/rand"
	"math"
	"os"
	"path/filepath"
	"sort"
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
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/querier/iterators"
)

// This builder uses TSDB's chunk and index writer directly, without
// using TSDB Head.
type tsdbBuilder struct {
	log log.Logger

	ulid        ulid.ULID
	outDir      string
	tmpBlockDir string

	chunksWriterMu sync.Mutex
	chunksWriter   tsdb.ChunkWriter

	startTime model.Time
	endTime   model.Time

	seriesMu sync.Mutex
	series   []series

	symbolsMap *symbolsMap

	writtenSamples  prometheus.Counter
	processedSeries prometheus.Counter
}

func newTsdbBuilder(outDir string, start, end time.Time, log log.Logger, processedSeries, writtenSamples prometheus.Counter) (*tsdbBuilder, error) {
	id, err := ulid.New(ulid.Now(), rand.Reader)
	if err != nil {
		return nil, errors.Wrap(err, "create ULID")
	}

	blockDir := filepath.Join(outDir, id.String()+".tmp")

	err = os.RemoveAll(blockDir)
	if err != nil {
		return nil, err
	}

	chunksWriter, err := chunks.NewWriter(filepath.Join(blockDir, "chunks"))
	if err != nil {
		return nil, errors.Wrap(err, "chunks writer")
	}

	return &tsdbBuilder{
		log:          log,
		ulid:         id,
		outDir:       outDir,
		tmpBlockDir:  blockDir,
		chunksWriter: chunksWriter,
		startTime:    model.TimeFromUnixNano(start.UnixNano()),
		endTime:      model.TimeFromUnixNano(end.UnixNano()),
		symbolsMap:   newSymbolsMap(),

		processedSeries: processedSeries,
		writtenSamples:  writtenSamples,
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

	d.chunksWriterMu.Lock()
	err = d.chunksWriter.WriteChunks(chs...)
	d.chunksWriterMu.Unlock()

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

	d.writtenSamples.Add(float64(seriesSamples))
	d.addSeries(metric, chs, seriesSamples, minTime, maxTime)
	return nil
}

func (d *tsdbBuilder) addSeries(m labels.Labels, cs []chunks.Meta, samples uint64, minTime, maxTime int64) {
	sl := d.symbolsMap.toSymbolLabels(m)

	d.seriesMu.Lock()
	defer d.seriesMu.Unlock()

	d.series = append(d.series, series{metric: sl, cs: cs, minTime: minTime, maxTime: maxTime, samples: samples})
}

func (d *tsdbBuilder) finishBlock(source string, labels map[string]string) (ulid.ULID, error) {
	if err := d.chunksWriter.Close(); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "closing chunks writer")
	}

	meta := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    d.ulid,
			Version: 1,
			MinTime: math.MaxInt64,
			MaxTime: math.MinInt64,
			Compaction: tsdb.BlockMetaCompaction{
				Level:   1,
				Sources: []ulid.ULID{d.ulid},
			},
		},

		Thanos: metadata.Thanos{
			Labels: labels,
			Source: metadata.SourceType(source),
		},
	}

	indexWriter, err := index.NewWriter(context.Background(), filepath.Join(d.tmpBlockDir, "index"))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "new index writer")
	}

	// Sort symbols, and add them to index.
	symbols := d.symbolsMap.getSortedSymbols()
	level.Info(d.log).Log("msg", "adding symbols to index", "count", len(symbols))

	for _, s := range symbols {
		if err := indexWriter.AddSymbol(s); err != nil {
			return ulid.ULID{}, errors.Wrapf(err, "adding symbol %v", s)
		}
	}

	level.Info(d.log).Log("msg", "adding series to index", "count", len(d.series))

	// Sort series lexicographically, and add them to index.
	sort.Slice(d.series, func(i, j int) bool {
		return d.symbolsMap.compareSymbolLabels(d.series[i].metric, d.series[j].metric) < 0
	})

	meta.MinTime = int64(d.startTime)
	meta.MaxTime = int64(d.endTime)

	for ix, s := range d.series {
		m := d.symbolsMap.toLabels(s.metric)

		if err := indexWriter.AddSeries(uint64(ix), m, s.cs...); err != nil {
			return ulid.ULID{}, errors.Wrapf(err, "adding series %v", m.String())
		}

		meta.Stats.NumSamples += s.samples
		meta.Stats.NumSeries++
		meta.Stats.NumChunks += uint64(len(s.cs))
	}

	if err := indexWriter.Close(); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "closing index writer")
	}

	if err := metadata.Write(d.log, d.tmpBlockDir, meta); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "writing meta.json")
	}

	if err := os.Rename(d.tmpBlockDir, filepath.Join(d.outDir, d.ulid.String())); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "rename to final directory")
	}

	return d.ulid, nil
}

type series struct {
	metric           []symbolLabel
	cs               []chunks.Meta
	minTime, maxTime int64
	samples          uint64
}
