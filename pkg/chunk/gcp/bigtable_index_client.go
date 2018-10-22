package gcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/go-kit/kit/log/level"
	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"

	"github.com/cortexproject/cortex/pkg/chunk"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/pkg/errors"
)

const (
	columnFamily = "f"
	columnPrefix = columnFamily + ":"
	column       = "c"
	separator    = "\000"
	maxRowReads  = 100
	null         = string('\xff')
)

// Config for a StorageClient
type Config struct {
	Project  string `yaml:"project"`
	Instance string `yaml:"instance"`

	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config"`

	ColumnKey      bool
	DistributeKeys bool
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Project, "bigtable.project", "", "Bigtable project ID.")
	f.StringVar(&cfg.Instance, "bigtable.instance", "", "Bigtable instance ID.")

	cfg.GRPCClientConfig.RegisterFlags("bigtable", f)
}

// storageClientColumnKey implements chunk.storageClient for GCP.
type storageClientColumnKey struct {
	cfg       Config
	schemaCfg chunk.SchemaConfig
	client    *bigtable.Client
	keysFn    keysFn

	distributeKeys bool
}

// storageClientV1 implements chunk.storageClient for GCP.
type storageClientV1 struct {
	storageClientColumnKey
}

// NewStorageClientV1 returns a new v1 StorageClient.
func NewStorageClientV1(ctx context.Context, cfg Config, schemaCfg chunk.SchemaConfig) (chunk.IndexClient, error) {
	opts := toOptions(cfg.GRPCClientConfig.DialOption(bigtableInstrumentation()))
	client, err := bigtable.NewClient(ctx, cfg.Project, cfg.Instance, opts...)
	if err != nil {
		return nil, err
	}
	return newStorageClientV1(cfg, schemaCfg, client), nil
}

func newStorageClientV1(cfg Config, schemaCfg chunk.SchemaConfig, client *bigtable.Client) *storageClientV1 {
	return &storageClientV1{
		storageClientColumnKey{
			cfg:       cfg,
			schemaCfg: schemaCfg,
			client:    client,
			keysFn: func(hashValue string, rangeValue []byte) (string, string) {
				rowKey := hashValue + separator + string(rangeValue)
				return rowKey, column
			},
		},
	}
}

// NewStorageClientColumnKey returns a new v2 StorageClient.
func NewStorageClientColumnKey(ctx context.Context, cfg Config, schemaCfg chunk.SchemaConfig) (chunk.IndexClient, error) {
	opts := toOptions(cfg.GRPCClientConfig.DialOption(bigtableInstrumentation()))
	client, err := bigtable.NewClient(ctx, cfg.Project, cfg.Instance, opts...)
	if err != nil {
		return nil, err
	}
	return newStorageClientColumnKey(cfg, schemaCfg, client), nil
}

func newStorageClientColumnKey(cfg Config, schemaCfg chunk.SchemaConfig, client *bigtable.Client) *storageClientColumnKey {

	return &storageClientColumnKey{
		cfg:       cfg,
		schemaCfg: schemaCfg,
		client:    client,
		keysFn: func(hashValue string, rangeValue []byte) (string, string) {

			// We hash the row key and prepend it back to the key for better distribution.
			// We preserve the existing key to make migrations and o11y easier.
			if cfg.DistributeKeys {
				hashValue = hashPrefix(hashValue) + "-" + hashValue
			}

			return hashValue, string(rangeValue)
		},
	}
}

// hashPrefix calculates a 64bit hash of the input string and hex-encodes
// the result, taking care to zero pad etc.
func hashPrefix(input string) string {
	prefix := hashAdd(hashNew(), input)
	var encodedUint64 [8]byte
	binary.LittleEndian.PutUint64(encodedUint64[:], prefix)
	var hexEncoded [16]byte
	hex.Encode(hexEncoded[:], encodedUint64[:])
	return string(hexEncoded[:])
}

func (s *storageClientColumnKey) Stop() {
	s.client.Close()
}

func (s *storageClientColumnKey) NewWriteBatch() chunk.WriteBatch {
	return bigtableWriteBatch{
		tables: map[string]map[string]*bigtable.Mutation{},
		keysFn: s.keysFn,
	}
}

// keysFn returns the row and column keys for the given hash and range keys.
type keysFn func(hashValue string, rangeValue []byte) (rowKey, columnKey string)

type bigtableWriteBatch struct {
	tables map[string]map[string]*bigtable.Mutation
	keysFn keysFn
}

func (b bigtableWriteBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	rows, ok := b.tables[tableName]
	if !ok {
		rows = map[string]*bigtable.Mutation{}
		b.tables[tableName] = rows
	}

	rowKey, columnKey := b.keysFn(hashValue, rangeValue)
	mutation, ok := rows[rowKey]
	if !ok {
		mutation = bigtable.NewMutation()
		rows[rowKey] = mutation
	}

	mutation.Set(columnFamily, columnKey, 0, value)
}

func (s *storageClientColumnKey) BatchWrite(ctx context.Context, batch chunk.WriteBatch) error {
	bigtableBatch := batch.(bigtableWriteBatch)

	for tableName, rows := range bigtableBatch.tables {
		table := s.client.Open(tableName)
		rowKeys := make([]string, 0, len(rows))
		muts := make([]*bigtable.Mutation, 0, len(rows))
		for rowKey, mut := range rows {
			rowKeys = append(rowKeys, rowKey)
			muts = append(muts, mut)
		}

		errs, err := table.ApplyBulk(ctx, rowKeys, muts)
		if err != nil {
			return err
		}
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *storageClientColumnKey) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) bool) error {
	sp, ctx := ot.StartSpanFromContext(ctx, "QueryPages")
	defer sp.Finish()

	// A limitation of this approach is that this only fetches whole rows; but
	// whatever, we filter them in the cache on the client.  But for unit tests to
	// pass, we must do this.
	callback = chunk_util.QueryFilter(callback)

	type tableQuery struct {
		name    string
		queries map[string]chunk.IndexQuery
		rows    bigtable.RowList
	}

	tableQueries := map[string]tableQuery{}
	for _, query := range queries {
		tq, ok := tableQueries[query.TableName]
		if !ok {
			tq = tableQuery{
				name:    query.TableName,
				queries: map[string]chunk.IndexQuery{},
			}
		}
		hashKey, _ := s.keysFn(query.HashValue, nil)
		tq.queries[hashKey] = query
		tq.rows = append(tq.rows, hashKey)
		tableQueries[query.TableName] = tq
	}

	errs := make(chan error)
	for _, tq := range tableQueries {
		table := s.client.Open(tq.name)

		for i := 0; i < len(tq.rows); i += maxRowReads {
			page := tq.rows[i:util.Min(i+maxRowReads, len(tq.rows))]
			go func(page bigtable.RowList, tq tableQuery) {
				var processingErr error
				// rows are returned in key order, not order in row list
				err := table.ReadRows(ctx, page, func(row bigtable.Row) bool {
					query, ok := tq.queries[row.Key()]
					if !ok {
						processingErr = errors.WithStack(fmt.Errorf("Got row for unknown chunk: %s", row.Key()))
						return false
					}

					val, ok := row[columnFamily]
					if !ok {
						// There are no matching rows.
						return true
					}

					return callback(query, &columnKeyBatch{
						items: val,
					})
				})

				if processingErr != nil {
					errs <- processingErr
				} else {
					errs <- err
				}
			}(page, tq)
		}
	}

	var lastErr error
	for _, tq := range tableQueries {
		for i := 0; i < len(tq.rows); i += maxRowReads {
			err := <-errs
			if err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

// columnKeyBatch represents a batch of values read from Bigtable.
type columnKeyBatch struct {
	items []bigtable.ReadItem
}

func (c *columnKeyBatch) Iterator() chunk.ReadBatchIterator {
	return &columnKeyIterator{
		i:              -1,
		columnKeyBatch: c,
	}
}

type columnKeyIterator struct {
	i int
	*columnKeyBatch
}

func (c *columnKeyIterator) Next() bool {
	c.i++
	return c.i < len(c.items)
}

func (c *columnKeyIterator) RangeValue() []byte {
	return []byte(strings.TrimPrefix(c.items[c.i].Column, columnPrefix))
}

func (c *columnKeyIterator) Value() []byte {
	return c.items[c.i].Value
}

func (s *storageClientV1) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) bool) error {
	return chunk_util.DoParallelQueries(ctx, s.query, queries, callback)
}

func (s *storageClientV1) query(ctx context.Context, query chunk.IndexQuery, callback func(result chunk.ReadBatch) (shouldContinue bool)) error {
	const null = string('\xff')

	sp, ctx := ot.StartSpanFromContext(ctx, "QueryPages", ot.Tag{Key: "tableName", Value: query.TableName}, ot.Tag{Key: "hashValue", Value: query.HashValue})
	defer sp.Finish()

	table := s.client.Open(query.TableName)

	var rowRange bigtable.RowRange

	/* Bigtable only seems to support regex match on cell values, so doing it
	   client side for now
	readOpts := []bigtable.ReadOption{
		bigtable.RowFilter(bigtable.FamilyFilter(columnFamily)),
	}
	if query.ValueEqual != nil {
		readOpts = append(readOpts, bigtable.RowFilter(bigtable.ValueFilter(string(query.ValueEqual))))
	}
	*/
	if len(query.RangeValuePrefix) > 0 {
		rowRange = bigtable.PrefixRange(query.HashValue + separator + string(query.RangeValuePrefix))
	} else if len(query.RangeValueStart) > 0 {
		rowRange = bigtable.NewRange(query.HashValue+separator+string(query.RangeValueStart), query.HashValue+separator+null)
	} else {
		rowRange = bigtable.PrefixRange(query.HashValue + separator)
	}

	err := table.ReadRows(ctx, rowRange, func(r bigtable.Row) bool {
		if query.ValueEqual == nil || bytes.Equal(r[columnFamily][0].Value, query.ValueEqual) {
			return callback(&rowBatch{
				row: r,
			})
		}

		return true
	})
	if err != nil {
		sp.LogFields(otlog.String("error", err.Error()))
		return errors.WithStack(err)
	}
	return nil
}

// rowBatch represents a batch of rows read from Bigtable.  As the
// bigtable interface gives us rows one-by-one, a batch always only contains
// a single row.
type rowBatch struct {
	row bigtable.Row
}

func (b *rowBatch) Iterator() chunk.ReadBatchIterator {
	return &rowBatchIterator{
		rowBatch: b,
	}
}

type rowBatchIterator struct {
	consumed bool
	*rowBatch
}

func (b *rowBatchIterator) Next() bool {
	if b.consumed {
		return false
	}
	b.consumed = true
	return true
}

func (b *rowBatchIterator) RangeValue() []byte {
	// String before the first separator is the hashkey
	parts := strings.SplitN(b.row.Key(), separator, 2)
	return []byte(parts[1])
}

func (b *rowBatchIterator) Value() []byte {
	cf, ok := b.row[columnFamily]
	if !ok || len(cf) != 1 {
		panic("bad response from bigtable")
	}
	return cf[0].Value
}

func (b *bigtableStreamer) Stream(ctx context.Context, out chan []chunk.Chunk) error {
	sp, ctx := ot.StartSpanFromContext(ctx, "Stream")
	defer sp.Finish()

	for _, query := range b.queries {
		level.Info(util.WithContext(ctx, util.Logger)).Log("msg", "streaming query", "queryID", query.id)
		var (
			table         = b.client.Open(query.table)
			processingErr error
		)
		decodeContext := chunk.NewDecodeContext()
		rr := query.generateRowRange()
		var chunks []chunk.Chunk
		var curKey string
		// Read through rows and forward slices of chunks with the same metrics
		// fingerprint
		err := table.ReadRows(ctx, rr, func(row bigtable.Row) bool {
			c, err := chunk.ParseExternalKey(query.userID, row.Key())
			if err != nil {
				processingErr = err
				return false
			}
			if c.Fingerprint.String() != curKey {
				out <- chunks
				curKey = c.Fingerprint.String()
				chunks = []chunk.Chunk{}
			}
			err = c.Decode(decodeContext, row[columnFamily][0].Value)
			if err != nil {
				processingErr = err
				return false
			}
			chunks = append(chunks, c)
			return true
		})
		if err != nil {
			return fmt.Errorf("stream canceled, %v. current query %v_%v, with user %v ", err, query.table, query.identifier, query.userID)
		}
		out <- chunks
		if processingErr != nil {
			return fmt.Errorf("stream canceled, %v. current query %v_%v, with user %v ", processingErr, query.table, query.identifier, query.userID)
		}
		for len(out) != 0 {
			continue
		}
	}
	return nil
}

func (b *bigtableStreamer) Size(ctx context.Context) (int, error) {
	n := 0
	for _, query := range b.queries {
		table := b.client.Open(query.table)
		rr := query.generateRowRange()

		err := table.ReadRows(ctx, rr, func(row bigtable.Row) bool {
			n++
			return true
		}, bigtable.RowFilter(bigtable.StripValueFilter()))
		if err != nil {
			return 0, fmt.Errorf("error: %v, query: %v_%v_%v", err, query.table, query.identifier, query.userID)
		}
	}
	return n, nil
}

// NewStreamer returns a GCP Bigtable specific stream batch.
// By design the batch needs a userID, Table, and two integers representing
// the first two characters of the fingerprint of metrics which will be streamed.
// stream. Shards are an integer between 0 and 240 that map onto 2 hex characters.
// For Example:
// 			Shard | Prefix
//			    0 | 10
//			    1 | 11
//			  ... | ...
//			   16 |
//			  240 | ff
//
// Technically there are 256 combinations of 2 hex character (16^2). However,
// fingerprints will not lead with a 0 character so 00->0f excluded, leading to
// 240
func (s *storageClientColumnKey) NewStreamer() chunk.Streamer {
	return &bigtableStreamer{
		client:  s.client,
		queries: []bigtableStreamQuery{},
	}
}

type bigtableStreamer struct {
	queries []bigtableStreamQuery
	client  *bigtable.Client
}

func (b *bigtableStreamer) Add(tableName, userID string, from, to int) {
	queryID := fmt.Sprintf("%v_%v_%v_%v", tableName, userID, from, to)
	if from == 1 && to == 240 {
		b.queries = append(b.queries, bigtableStreamQuery{queryID, userID, "", tableName})
		return
	}
	for i := from; i < to; i++ {
		prefix := fmt.Sprintf("%02x", i+16)
		b.queries = append(b.queries, bigtableStreamQuery{queryID, userID, prefix, tableName})
	}
}

type bigtableStreamQuery struct {
	id         string
	userID     string
	identifier string
	table      string
}

func (b bigtableStreamQuery) generateRowRange() bigtable.RowRange {
	return bigtable.PrefixRange(b.userID + "/" + b.identifier)
}
