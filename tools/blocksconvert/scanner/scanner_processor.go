package scanner

import (
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/tools/blocksconvert"
)

// Results from processor are passed to this function.
type planEntryFn func(dir string, file string, entry blocksconvert.PlanEntry, header func() blocksconvert.PlanEntry) error

// Processor implements IndexEntryProcessor. It caches chunks for single series until it finds
// that another series has arrived, at which point it writes it to the file.
// IndexReader guarantees correct order of entries.
type processor struct {
	dir      string
	resultFn planEntryFn

	series  prometheus.Counter
	scanned *prometheus.CounterVec

	allowedUsers      blocksconvert.AllowedUsers
	ignoredUsersRegex *regexp.Regexp
	ignoredUsers      map[string]struct{}
	ignoredEntries    prometheus.Counter

	lastKey key
	chunks  []string
}

// Key is full series ID, used by processor to find out whether subsequent index entries belong to the same series
// or not.
type key struct {
	user     string
	dayIndex int
	seriesID string
}

func newProcessor(dir string, resultFn planEntryFn, allowed blocksconvert.AllowedUsers, ignoredUsers *regexp.Regexp, series prometheus.Counter, scannedEntries *prometheus.CounterVec, ignoredEntries prometheus.Counter) *processor {
	w := &processor{
		dir:      dir,
		resultFn: resultFn,
		series:   series,
		scanned:  scannedEntries,

		allowedUsers:      allowed,
		ignoredUsersRegex: ignoredUsers,
		ignoredUsers:      map[string]struct{}{},
		ignoredEntries:    ignoredEntries,
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

	if !w.AcceptUser(user) {
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
			return errors.Wrap(err, "failed to flush chunks")
		}
	}

	w.lastKey = k
	w.chunks = append(w.chunks, chunkID)
	return nil
}

func (w *processor) AcceptUser(user string) bool {
	if _, found := w.ignoredUsers[user]; found {
		w.ignoredEntries.Inc()
		return false
	}
	if !w.allowedUsers.IsAllowed(user) || (w.ignoredUsersRegex != nil && w.ignoredUsersRegex.MatchString(user)) {
		w.ignoredEntries.Inc()
		w.ignoredUsers[user] = struct{}{}
		return false
	}
	return true
}

func (w *processor) Flush() error {
	if len(w.chunks) == 0 {
		return nil
	}

	k := w.lastKey

	err := w.resultFn(filepath.Join(w.dir, k.user), strconv.Itoa(k.dayIndex)+".plan", blocksconvert.PlanEntry{
		SeriesID: w.lastKey.seriesID,
		Chunks:   w.chunks,
	}, func() blocksconvert.PlanEntry {
		return blocksconvert.PlanEntry{
			User:     k.user,
			DayIndex: k.dayIndex,
		}
	})

	if err != nil {
		return err
	}

	w.series.Inc()
	w.chunks = nil
	return nil
}
