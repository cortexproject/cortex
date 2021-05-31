package scanner

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cassandra"
)

/* Cassandra can easily run out of memory or timeout if we try to SELECT the
 * entire table. Splitting into many smaller chunks help a lot. */
const nbTokenRanges = 512
const queryPageSize = 10000

type cassandraIndexReader struct {
	log                    log.Logger
	cassandraStorageConfig cassandra.Config
	schemaCfg              chunk.SchemaConfig

	rowsRead                  prometheus.Counter
	parsedIndexEntries        prometheus.Counter
	currentTableRanges        prometheus.Gauge
	currentTableScannedRanges prometheus.Gauge
}

func newCassandraIndexReader(cfg cassandra.Config, schemaCfg chunk.SchemaConfig, l log.Logger, rowsRead prometheus.Counter, parsedIndexEntries prometheus.Counter, currentTableRanges, scannedRanges prometheus.Gauge) *cassandraIndexReader {
	return &cassandraIndexReader{
		log:                    l,
		cassandraStorageConfig: cfg,

		rowsRead:                  rowsRead,
		parsedIndexEntries:        parsedIndexEntries,
		currentTableRanges:        currentTableRanges,
		currentTableScannedRanges: scannedRanges,
	}
}

func (r *cassandraIndexReader) IndexTableNames(ctx context.Context) ([]string, error) {
	client, err := cassandra.NewTableClient(ctx, r.cassandraStorageConfig, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create cassandra client failed")
	}

	defer client.Stop()

	return client.ListTables(ctx)
}

type tokenRange struct {
	start int64
	end   int64
}

func (r *cassandraIndexReader) ReadIndexEntries(ctx context.Context, tableName string, processors []chunk.IndexEntryProcessor) error {
	level.Debug(r.log).Log("msg", "scanning table", "table", tableName)

	client, err := cassandra.NewStorageClient(r.cassandraStorageConfig, r.schemaCfg, nil)
	if err != nil {
		return errors.Wrap(err, "create cassandra storage client failed")
	}

	defer client.Stop()

	session := client.GetReadSession()

	rangesCh := make(chan tokenRange, nbTokenRanges)

	var step, n, start int64

	step = int64(math.MaxUint64 / nbTokenRanges)

	for n = 0; n < nbTokenRanges; n++ {
		start = math.MinInt64 + n*step
		end := start + step

		if n == (nbTokenRanges - 1) {
			end = math.MaxInt64
		}

		t := tokenRange{start: start, end: end}
		rangesCh <- t
	}

	close(rangesCh)

	r.currentTableRanges.Set(float64(len(rangesCh)))
	r.currentTableScannedRanges.Set(0)

	defer r.currentTableRanges.Set(0)
	defer r.currentTableScannedRanges.Set(0)

	g, gctx := errgroup.WithContext(ctx)

	for ix := range processors {
		p := processors[ix]
		g.Go(func() error {
			for rng := range rangesCh {
				level.Debug(r.log).Log("msg", "reading rows", "range_start", rng.start, "range_end", rng.end, "table_name", tableName)

				query := fmt.Sprintf("SELECT hash, range, value FROM %s WHERE token(hash) >= %v", tableName, rng.start)

				if rng.end < math.MaxInt64 {
					query += fmt.Sprintf(" AND token(hash) < %v", rng.end)
				}

				iter := session.Query(query).WithContext(gctx).PageSize(queryPageSize).Iter()

				if len(iter.Warnings()) > 0 {
					level.Warn(r.log).Log("msg", "warnings from cassandra", "warnings", strings.Join(iter.Warnings(), " :: "))
				}

				scanner := iter.Scanner()

				oldHash := ""
				oldRng := ""

				for scanner.Next() {
					var hash, rng, value string

					err := scanner.Scan(&hash, &rng, &value)
					if err != nil {
						return errors.Wrap(err, "Cassandra scan error")
					}

					r.rowsRead.Inc()
					r.parsedIndexEntries.Inc()

					entry := chunk.IndexEntry{
						TableName:  tableName,
						HashValue:  hash,
						RangeValue: []byte(rng),
						Value:      []byte(value),
					}

					if rng < oldRng && oldHash == hash {
						level.Error(r.log).Log("msg", "new rng bad", "rng", rng, "old_rng", oldRng, "hash", hash, "old_hash", oldHash)
						return fmt.Errorf("received range row in the wrong order for same hash: %v < %v", rng, oldRng)
					}

					err = p.ProcessIndexEntry(entry)
					if err != nil {
						return errors.Wrap(err, "processor error")
					}

					oldHash = hash
					oldRng = rng
				}

				// This will also close the iterator.
				err := scanner.Err()
				if err != nil {
					return errors.Wrap(err, "Cassandra error during scan")
				}

				r.currentTableScannedRanges.Inc()
			}

			return p.Flush()
		})
	}

	return g.Wait()
}
