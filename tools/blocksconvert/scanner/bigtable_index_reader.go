package scanner

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"
)

type bigtableIndexReader struct {
	log      log.Logger
	project  string
	instance string

	rowsRead                  prometheus.Counter
	parsedIndexEntries        prometheus.Counter
	currentTableRanges        prometheus.Gauge
	currentTableScannedRanges prometheus.Gauge
}

func newBigtableIndexReader(project, instance string, l log.Logger, rowsRead prometheus.Counter, parsedIndexEntries prometheus.Counter, currentTableRanges, scannedRanges prometheus.Gauge) *bigtableIndexReader {
	return &bigtableIndexReader{
		log:      l,
		project:  project,
		instance: instance,

		rowsRead:                  rowsRead,
		parsedIndexEntries:        parsedIndexEntries,
		currentTableRanges:        currentTableRanges,
		currentTableScannedRanges: scannedRanges,
	}
}

func (r *bigtableIndexReader) IndexTableNames(ctx context.Context) ([]string, error) {
	client, err := bigtable.NewAdminClient(ctx, r.project, r.instance)
	if err != nil {
		return nil, errors.Wrap(err, "create bigtable client failed")
	}
	defer closeCloser(r.log, "bigtable admin client", client)

	return client.Tables(ctx)
}

// This reader supports both used versions of BigTable index client used by Cortex:
//
// 1) newStorageClientV1 ("gcp"), which sets
//    - RowKey = entry.HashValue + \0 + entry.RangeValue
//    - Column: "c" (in family "f")
//    - Value: entry.Value
//
// 2) newStorageClientColumnKey ("gcp-columnkey", "bigtable", "bigtable-hashed"), which has two possibilities:
//    - RowKey = entry.HashValue OR (if distribute key flag is enabled) hashPrefix(entry.HashValue) + "-" + entry.HashValue, where hashPrefix is 64-bit FNV64a hash, encoded as little-endian hex value
//    - Column: entry.RangeValue (in family "f")
//    - Value: entry.Value
//
// Index entries are returned in HashValue, RangeValue order.
// Entries for the same HashValue and RangeValue are passed to the same processor.
func (r *bigtableIndexReader) ReadIndexEntries(ctx context.Context, tableName string, processors []chunk.IndexEntryProcessor) error {
	client, err := bigtable.NewClient(ctx, r.project, r.instance)
	if err != nil {
		return errors.Wrap(err, "create bigtable client failed")
	}
	defer closeCloser(r.log, "bigtable client", client)

	var rangesCh chan bigtable.RowRange

	tbl := client.Open(tableName)
	if keys, err := tbl.SampleRowKeys(ctx); err == nil {
		level.Info(r.log).Log("msg", "sampled row keys", "keys", strings.Join(keys, ", "))

		rangesCh = make(chan bigtable.RowRange, len(keys)+1)

		start := ""
		for _, k := range keys {
			rangesCh <- bigtable.NewRange(start, k)
			start = k
		}
		rangesCh <- bigtable.InfiniteRange(start) // Last segment from last key, to the end.
		close(rangesCh)
	} else {
		level.Warn(r.log).Log("msg", "failed to sample row keys", "err", err)

		rangesCh = make(chan bigtable.RowRange, 1)
		rangesCh <- bigtable.InfiniteRange("")
		close(rangesCh)
	}

	r.currentTableRanges.Set(float64(len(rangesCh)))
	r.currentTableScannedRanges.Set(0)

	defer r.currentTableRanges.Set(0)
	defer r.currentTableScannedRanges.Set(0)

	g, gctx := errgroup.WithContext(ctx)

	for ix := range processors {
		p := processors[ix]

		g.Go(func() error {
			for rng := range rangesCh {
				var innerErr error

				level.Info(r.log).Log("msg", "reading rows", "range", rng)

				err := tbl.ReadRows(gctx, rng, func(row bigtable.Row) bool {
					r.rowsRead.Inc()

					entries, err := parseRowKey(row, tableName)
					if err != nil {
						innerErr = errors.Wrapf(err, "failed to parse row: %s", row.Key())
						return false
					}

					r.parsedIndexEntries.Add(float64(len(entries)))

					for _, e := range entries {
						err := p.ProcessIndexEntry(e)
						if err != nil {
							innerErr = errors.Wrap(err, "processor error")
							return false
						}
					}

					return true
				})

				if innerErr != nil {
					return innerErr
				}

				if err != nil {
					return err
				}

				r.currentTableScannedRanges.Inc()
			}

			return p.Flush()
		})
	}

	return g.Wait()
}

func parseRowKey(row bigtable.Row, tableName string) ([]chunk.IndexEntry, error) {
	var entries []chunk.IndexEntry

	rowKey := row.Key()

	rangeInRowKey := false
	hashValue := row.Key()
	rangeValue := ""

	// Remove hashPrefix, if used. Easy to check.
	if len(hashValue) > 16 && hashValue[16] == '-' && hashValue[:16] == gcp.HashPrefix(hashValue[17:]) {
		hashValue = hashValue[17:]
	} else if ix := strings.IndexByte(hashValue, 0); ix > 0 {
		// newStorageClientV1 uses
		//    - RowKey: entry.HashValue + \0 + entry.RangeValue
		//    - Column: "c" (in family "f")
		//    - Value: entry.Value

		rangeInRowKey = true
		rangeValue = hashValue[ix+1:]
		hashValue = hashValue[:ix]
	}

	for family, columns := range row {
		if family != "f" {
			return nil, errors.Errorf("unknown family: %s", family)
		}

		for _, colVal := range columns {
			if colVal.Row != rowKey {
				return nil, errors.Errorf("rowkey mismatch: %q, %q", colVal.Row, rowKey)
			}

			if rangeInRowKey {
				if colVal.Column != "f:c" {
					return nil, errors.Errorf("found rangeValue in RowKey, but column is not 'f:c': %q", colVal.Column)
				}
				// we already have rangeValue
			} else {
				if !strings.HasPrefix(colVal.Column, "f:") {
					return nil, errors.Errorf("invalid column prefix: %q", colVal.Column)
				}
				rangeValue = colVal.Column[2:] // With "f:" part removed
			}

			entry := chunk.IndexEntry{
				TableName:  tableName,
				HashValue:  hashValue,
				RangeValue: []byte(rangeValue),
				Value:      colVal.Value,
			}

			entries = append(entries, entry)
		}
	}

	if len(entries) > 1 {
		// Sort entries by RangeValue. This is done to support `newStorageClientColumnKey` version properly:
		// all index entries with same hashValue are in the same row, but map iteration over columns may
		// have returned them in wrong order.

		sort.Sort(sortableIndexEntries(entries))
	}

	return entries, nil
}

func closeCloser(log log.Logger, closerName string, closer io.Closer) {
	err := closer.Close()
	if err != nil {
		level.Warn(log).Log("msg", "failed to close "+closerName, "err", err)
	}
}

type sortableIndexEntries []chunk.IndexEntry

func (s sortableIndexEntries) Len() int {
	return len(s)
}

func (s sortableIndexEntries) Less(i, j int) bool {
	return bytes.Compare(s[i].RangeValue, s[j].RangeValue) < 0
}

func (s sortableIndexEntries) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
