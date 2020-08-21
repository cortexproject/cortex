package blocksconvert

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"
)

type bigtableIndexReader struct {
	log      log.Logger
	project  string
	instance string

	rowsRead           prometheus.Counter
	parsedIndexEntries prometheus.Counter
}

func NewBigtableIndexReader(project, instance string, l log.Logger, reg prometheus.Registerer) *bigtableIndexReader {
	return &bigtableIndexReader{
		log:      l,
		project:  project,
		instance: instance,

		rowsRead: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "bigtable_read_rows_total",
			Help: "Number of rows read from BigTable",
		}),
		parsedIndexEntries: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "bigtable_parsed_index_entries_total",
			Help: "Number of parsed index entries",
		}),
	}
}

func (r *bigtableIndexReader) IndexTableNames(ctx context.Context) ([]string, error) {
	client, err := bigtable.NewAdminClient(ctx, r.project, r.instance)
	if err != nil {
		return nil, fmt.Errorf("create bigtable client failed: %w", err)
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
//    - RowKey = entry.HashValue OR (if distribute key flag is enabled) hashPrefix(entry.HashValue) + "-" + entry.HashValue, where hashPrefix is 64-bit FNV64a hash, encoded as little-endian
//    - Column: entry.RangeValue (in family "f")
//    - Value: entry.Value
//
// Index entries are returned in HashValue, RangeValue order.
// Entries for the same HashValue and RangeValue
func (r *bigtableIndexReader) ReadIndexEntries(ctx context.Context, tableName string, processors []IndexEntryProcessor) error {
	client, err := bigtable.NewClient(ctx, r.project, r.instance)
	if err != nil {
		return fmt.Errorf("create bigtable client failed: %w", err)
	}
	defer closeCloser(r.log, "bigtable client", client)

	var rangesCh chan bigtable.RowRange

	tbl := client.Open(tableName)
	if keys, err := tbl.SampleRowKeys(ctx); err == nil {
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

		rangesCh = make(chan bigtable.RowRange)
		rangesCh <- bigtable.InfiniteRange("")
		close(rangesCh)
	}

	g, gctx := errgroup.WithContext(ctx)

	for ix := range processors {
		p := processors[ix]

		g.Go(func() error {
			for rng := range rangesCh {
				var innerErr error

				err = tbl.ReadRows(gctx, rng, func(row bigtable.Row) bool {
					r.rowsRead.Inc()

					entries, err := parseRowKey(row, tableName)
					if err != nil {
						innerErr = fmt.Errorf("failed to parse row: %s: %w", row.Key(), err)
						return false
					}

					r.parsedIndexEntries.Add(float64(len(entries)))

					for _, e := range entries {
						err := p.ProcessIndexEntry(e)
						if err != nil {
							innerErr = fmt.Errorf("processor error: %w", err)
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
			}

			return p.Flush()
		})
	}

	return g.Wait()
}

func parseRowKey(row bigtable.Row, tableName string) ([]chunk.IndexEntry, error) {
	var entries []chunk.IndexEntry

	rangeInRowKey := false
	hashValue := row.Key()
	rangeValue := ""

	// Remove hashPrefix, if used. Easy to check.
	if len(hashValue) > 16 && hashValue[16] == '-' && hashValue[:16] == gcp.HashPrefix(hashValue[17:]) {
		hashValue = hashValue[17:]
	} else if ix := strings.IndexByte(hashValue, 0); ix > 0 {
		rangeInRowKey = true
		rangeValue = hashValue[ix+1:]
		hashValue = hashValue[:ix]
	}

	for family, columns := range row {
		if family != "f" {
			return nil, fmt.Errorf("unknown family: %s", family)
		}

		for _, colVal := range columns {
			if rangeInRowKey {
				if colVal.Column != "f:c" {
					return nil, fmt.Errorf("found rangeValue in RowKey, but column is not 'f:c': %q", colVal.Column)
				}
				// we already have rangeValue
			} else {
				if !strings.HasPrefix(colVal.Column, "f:") {
					return nil, fmt.Errorf("invalid column prefix: %q", colVal.Column)
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
