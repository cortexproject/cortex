package blocksconvert

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/chunk"
)

type key struct {
	user     string
	dayIndex int
	seriesID string
}

// Processor implements IndexEntryProcessor. It caches chunks for single series until it finds
// that another series has arrived, at which point it writes it to the file.
// IndexReader guarantees correct order of entries.
type processor struct {
	dir   string
	files *openFiles

	series prometheus.Counter

	lastKey key
	chunks  []string
}

func newProcessor(dir string, files *openFiles, series prometheus.Counter) *processor {
	w := &processor{
		dir:    dir,
		files:  files,
		series: series,
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
	}

	w.lastKey = k
	w.chunks = append(w.chunks, chunkID)
	return nil
}

func (w *processor) Flush() error {
	if len(w.chunks) == 0 {
		return nil
	}

	k := w.lastKey

	err := w.files.appendJsonEntryToFile(filepath.Join(w.dir, k.user), strconv.Itoa(k.dayIndex)+".plan", PlanEntry{
		SeriesID: w.lastKey.seriesID,
		Chunks:   w.chunks,
	}, func() interface{} {
		return PlanHeader{
			User:     k.user,
			DayIndex: k.dayIndex,
		}
	})

	if err != nil {
		return err
	}

	w.series.Inc()
	w.chunks = w.chunks[:0]
	return nil
}
