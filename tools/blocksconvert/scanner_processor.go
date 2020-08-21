package blocksconvert

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/chunk"
)

type key struct {
	user     string
	dayIndex int
	seriesID string
}

type processor struct {
	dir   string
	files *openFiles

	series prometheus.Counter

	lastKey key
	chunks  []string
}

func newProcessor(dir string, files *openFiles, reg prometheus.Registerer) *processor {
	w := &processor{
		dir:   dir,
		files: files,

		series: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "scanner_series_written_total",
			Help: "Number of series written to the plan files",
		}),
	}

	return w
}

func (w *processor) ProcessIndexEntry(indexEntry chunk.IndexEntry) error {
	if !IsSeriesToChunkMapping(indexEntry.RangeValue) {
		return nil
	}

	user, index, seriesID, chunkID, err := GetSeriesToChunkMapping(indexEntry.HashValue, indexEntry.RangeValue)
	if err != nil {
		return err
	}

	k := key{
		user:     user,
		dayIndex: index,
		seriesID: seriesID,
	}

	if w.lastKey != k && len(w.chunks) > 0 {
		err := w.Flush()
		if err != nil {
			return fmt.Errorf("failed to flush chunks: %w", err)
		}

		w.lastKey = k
	}

	w.chunks = append(w.chunks, chunkID)
	return nil
}

func (w *processor) Flush() error {
	if len(w.chunks) == 0 {
		return nil
	}

	err := w.files.appendJsonEntryToFile(filepath.Join(w.dir, w.lastKey.user), strconv.Itoa(w.lastKey.dayIndex)+".plan", PlanEntry{
		SeriesID: w.lastKey.seriesID,
		Chunks:   w.chunks,
	})

	if err != nil {
		return err
	}

	w.series.Inc()
	w.chunks = w.chunks[:0]
	return nil
}
