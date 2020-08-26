package scanner

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/tools/blocksconvert"
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
	dir     string
	files   *openFiles
	ignored *regexp.Regexp

	series  prometheus.Counter
	scanned *prometheus.CounterVec

	lastKey key
	chunks  []string
}

func newProcessor(dir string, files *openFiles, ignored *regexp.Regexp, series prometheus.Counter, scannedEntries *prometheus.CounterVec) *processor {
	w := &processor{
		dir:     dir,
		files:   files,
		series:  series,
		scanned: scannedEntries,
		ignored: ignored,
	}

	return w
}

func (w *processor) ProcessIndexEntry(indexEntry chunk.IndexEntry) error {
	switch {
	case IsMetricToSeriesMapping(indexEntry.RangeValue):
		w.scanned.WithLabelValues("metric-to-series").Inc()
		return nil

	case IsMetricLabelToLabelValueMapping(indexEntry.RangeValue):
		w.scanned.WithLabelValues("metric-label-to-label-value").Inc()
		return nil

	case IsSeriesToLabelValues(indexEntry.RangeValue):
		w.scanned.WithLabelValues("series-to-label-values").Inc()
		return nil

	case IsSeriesToChunkMapping(indexEntry.RangeValue):
		w.scanned.WithLabelValues("series-to-chunk").Inc()
		// We will process these, don't return yet.

	default:
		// Should not happen.
		w.scanned.WithLabelValues("unknown-" + UnknownIndexEntryType(indexEntry.RangeValue)).Inc()
		return nil
	}

	user, index, seriesID, chunkID, err := GetSeriesToChunkMapping(indexEntry.HashValue, indexEntry.RangeValue)
	if err != nil {
		return err
	}

	if w.ignored != nil && w.ignored.MatchString(user) {
		// Ignore this user.
		return nil
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

	err := w.files.appendJsonEntryToFile(filepath.Join(w.dir, k.user), strconv.Itoa(k.dayIndex)+".plan", blocksconvert.PlanEntry{
		SeriesID: w.lastKey.seriesID,
		Chunks:   w.chunks,
	}, func() interface{} {
		return blocksconvert.PlanEntry{
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
