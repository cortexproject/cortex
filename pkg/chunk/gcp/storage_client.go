package gcp

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"cloud.google.com/go/bigtable"
	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"

	"github.com/pkg/errors"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	columnFamily = "f"
	column       = "c"
	separator    = "\000"
	maxRowReads  = 100
)

// Config for a StorageClient
type Config struct {
	project  string
	instance string
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.project, "bigtable.project", "", "Bigtable project ID.")
	f.StringVar(&cfg.instance, "bigtable.instance", "", "Bigtable instance ID.")
}

type storageClientV1 struct {
	storageClientV2
}

// storageClientV2 implements chunk.storageClientV2 for GCP.
type storageClientV2 struct {
	cfg       Config
	schemaCfg chunk.SchemaConfig
	client    *bigtable.Client
}

// NewStorageClient returns a new StorageClient.
func NewStorageClient(ctx context.Context, cfg Config, schemaCfg chunk.SchemaConfig) (chunk.StorageClient, error) {
	return NewStorageClientV2(ctx, cfg, schemaCfg)
}

// NewStorageClientV1 returns a new v1 StorageClient.
func NewStorageClientV1(ctx context.Context, cfg Config, schemaCfg chunk.SchemaConfig) (chunk.StorageClient, error) {
	client, err := bigtable.NewClient(ctx, cfg.project, cfg.instance, instrumentation()...)
	if err != nil {
		return nil, err
	}
	return &storageClientV1{
		storageClientV2{
			cfg:       cfg,
			schemaCfg: schemaCfg,
			client:    client,
		},
	}, nil
}

// NewStorageClientV2 returns a new v2 StorageClient.
func NewStorageClientV2(ctx context.Context, cfg Config, schemaCfg chunk.SchemaConfig) (chunk.StorageClient, error) {
	client, err := bigtable.NewClient(ctx, cfg.project, cfg.instance, instrumentation()...)
	if err != nil {
		return nil, err
	}
	return &storageClientV2{
		cfg:       cfg,
		schemaCfg: schemaCfg,
		client:    client,
	}, nil
}

func (s *storageClientV2) NewWriteBatch() chunk.WriteBatch {
	return bigtableWriteBatchV2{
		tables: map[string]map[string]*bigtable.Mutation{},
	}
}

type bigtableWriteBatchV2 struct {
	tables map[string]map[string]*bigtable.Mutation
}

func (b bigtableWriteBatchV2) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	rows, ok := b.tables[tableName]
	if !ok {
		rows = map[string]*bigtable.Mutation{}
		b.tables[tableName] = rows
	}

	// TODO the hashValue should actually be hashed - but I have data written in
	// this format, so we need to do a proper migration.
	// TODO TODO ^
	rowKey := hashValue
	mutation, ok := rows[rowKey]
	if !ok {
		mutation = bigtable.NewMutation()
		rows[rowKey] = mutation
	}

	mutation.Set(columnFamily, string(rangeValue), 0, value)
}

func (s *storageClientV2) BatchWrite(ctx context.Context, batch chunk.WriteBatch) error {
	bigtableBatch := batch.(bigtableWriteBatchV2)

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

func (s *storageClientV2) QueryPages(ctx context.Context, query chunk.IndexQuery, callback func(result chunk.ReadBatch, lastPage bool) (shouldContinue bool)) error {
	sp, ctx := ot.StartSpanFromContext(ctx, "QueryPages", ot.Tag{Key: "tableName", Value: query.TableName}, ot.Tag{Key: "hashValue", Value: query.HashValue})
	defer sp.Finish()

	table := s.client.Open(query.TableName)

	rOpts := []bigtable.ReadOption{
		bigtable.RowFilter(bigtable.FamilyFilter(columnFamily)),
	}

	if len(query.RangeValuePrefix) > 0 {
		rOpts = append(rOpts, bigtable.RowFilter(bigtable.ColumnFilter(string(query.RangeValuePrefix)+".*"))) // TODO: Check again and anchor.
	} else if len(query.RangeValueStart) > 0 {
		rOpts = append(rOpts, bigtable.RowFilter(bigtable.ColumnRangeFilter(columnFamily, string(query.RangeValueStart), "")))
	}

	r, err := table.ReadRow(ctx, query.HashValue, rOpts...)
	if err != nil {
		sp.LogFields(otlog.String("error", err.Error()))
		return errors.WithStack(err)
	}

	val, ok := r[columnFamily]
	if !ok {
		panic("bad response from bigtable, columnFamily missing")
	}

	for i := range val {
		val[i].Column = strings.TrimPrefix(val[i].Column, columnFamily+":")
		// TODO: Hacky hacky ^
	}
	callback(bigtableReadBatchV2(val), true) // TODO: Check true or false.
	return nil
}

// bigtableReadBatchV2 represents a batch of values read from Bigtable.
type bigtableReadBatchV2 []bigtable.ReadItem

func (b bigtableReadBatchV2) Len() int {
	return len(b)
}

func (b bigtableReadBatchV2) RangeValue(index int) []byte {
	return []byte(b[index].Column)
}

func (b bigtableReadBatchV2) Value(index int) []byte {
	return b[index].Value
}

func (s *storageClientV2) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	keys := map[string][]string{}
	muts := map[string][]*bigtable.Mutation{}

	for i := range chunks {
		// Encode the chunk first - checksum is calculated as a side effect.
		buf, err := chunks[i].Encode()
		if err != nil {
			return err
		}
		key := chunks[i].ExternalKey()
		tableName := s.schemaCfg.ChunkTables.TableFor(chunks[i].From)
		keys[tableName] = append(keys[tableName], key)

		mut := bigtable.NewMutation()
		mut.Set(columnFamily, column, 0, buf)
		muts[tableName] = append(muts[tableName], mut)
	}

	for tableName := range keys {
		table := s.client.Open(tableName)
		errs, err := table.ApplyBulk(ctx, keys[tableName], muts[tableName])
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

func (s *storageClientV2) GetChunks(ctx context.Context, input []chunk.Chunk) ([]chunk.Chunk, error) {
	sp, ctx := ot.StartSpanFromContext(ctx, "GetChunks")
	defer sp.Finish()
	sp.LogFields(otlog.Int("chunks requested", len(input)))

	chunks := map[string]map[string]chunk.Chunk{}
	keys := map[string]bigtable.RowList{}
	for _, c := range input {
		tableName := s.schemaCfg.ChunkTables.TableFor(c.From)
		key := c.ExternalKey()
		keys[tableName] = append(keys[tableName], key)
		if _, ok := chunks[tableName]; !ok {
			chunks[tableName] = map[string]chunk.Chunk{}
		}
		chunks[tableName][key] = c
	}

	outs := make(chan chunk.Chunk, len(input))
	errs := make(chan error, len(input))

	for tableName := range keys {
		var (
			table  = s.client.Open(tableName)
			keys   = keys[tableName]
			chunks = chunks[tableName]
		)

		for i := 0; i < len(keys); i += maxRowReads {
			page := keys[i:util.Min(i+maxRowReads, len(keys))]
			go func(page bigtable.RowList) {
				decodeContext := chunk.NewDecodeContext()

				var processingErr error
				var recievedChunks = 0

				// rows are returned in key order, not order in row list
				err := table.ReadRows(ctx, page, func(row bigtable.Row) bool {
					chunk, ok := chunks[row.Key()]
					if !ok {
						processingErr = errors.WithStack(fmt.Errorf("Got row for unknown chunk: %s", row.Key()))
						return false
					}

					err := chunk.Decode(decodeContext, row[columnFamily][0].Value)
					if err != nil {
						processingErr = err
						return false
					}

					recievedChunks++
					outs <- chunk
					return true
				})

				if processingErr != nil {
					errs <- processingErr
				} else if err != nil {
					errs <- errors.WithStack(err)
				} else if recievedChunks < len(page) {
					errs <- errors.WithStack(fmt.Errorf("Asked for %d chunks for BigTable, received %d", len(page), recievedChunks))
				}
			}(page)
		}
	}

	output := make([]chunk.Chunk, 0, len(input))
	for i := 0; i < len(input); i++ {
		select {
		case c := <-outs:
			output = append(output, c)
		case err := <-errs:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return output, nil
}

func (s *storageClientV1) NewWriteBatch() chunk.WriteBatch {
	return bigtableWriteBatchV1{
		tables: map[string]map[string]*bigtable.Mutation{},
	}
}

type bigtableWriteBatchV1 struct {
	tables map[string]map[string]*bigtable.Mutation
}

func (b bigtableWriteBatchV1) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	rows, ok := b.tables[tableName]
	if !ok {
		rows = map[string]*bigtable.Mutation{}
		b.tables[tableName] = rows
	}

	// TODO the hashValue should actually be hashed - but I have data written in
	// this format, so we need to do a proper migration.
	rowKey := hashValue + separator + string(rangeValue)
	mutation, ok := rows[rowKey]
	if !ok {
		mutation = bigtable.NewMutation()
		rows[rowKey] = mutation
	}

	mutation.Set(columnFamily, column, 0, value)
}

func (s *storageClientV1) BatchWrite(ctx context.Context, batch chunk.WriteBatch) error {
	bigtableBatch := batch.(bigtableWriteBatchV1)

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

func (s *storageClientV1) QueryPages(ctx context.Context, query chunk.IndexQuery, callback func(result chunk.ReadBatch, lastPage bool) (shouldContinue bool)) error {
	sp, ctx := ot.StartSpanFromContext(ctx, "QueryPages", ot.Tag{Key: "tableName", Value: query.TableName}, ot.Tag{Key: "hashValue", Value: query.HashValue})
	defer sp.Finish()

	table := s.client.Open(query.TableName)

	var rowRange bigtable.RowRange
	if len(query.RangeValuePrefix) > 0 {
		rowRange = bigtable.PrefixRange(query.HashValue + separator + string(query.RangeValuePrefix))
	} else if len(query.RangeValueStart) > 0 {
		rowRange = bigtable.NewRange(query.HashValue+separator+string(query.RangeValueStart), query.HashValue+separator+string('\xff'))
	} else {
		rowRange = bigtable.PrefixRange(query.HashValue + separator)
	}

	err := table.ReadRows(ctx, rowRange, func(r bigtable.Row) bool {
		return callback(bigtableReadBatchV1(r), false)
	}, bigtable.RowFilter(bigtable.FamilyFilter(columnFamily)))
	if err != nil {
		sp.LogFields(otlog.String("error", err.Error()))
		return errors.WithStack(err)
	}
	return nil
}

// bigtableReadBatchV1 represents a batch of rows read from Bigtable.  As the
// bigtable interface gives us rows one-by-one, a batch always only contains
// a single row.
type bigtableReadBatchV1 bigtable.Row

func (bigtableReadBatchV1) Len() int {
	return 1
}

func (b bigtableReadBatchV1) RangeValue(index int) []byte {
	if index != 0 {
		panic("index != 0")
	}
	// String before the first separator is the hashkey
	parts := strings.SplitN(bigtable.Row(b).Key(), separator, 2)
	return []byte(parts[1])
}

func (b bigtableReadBatchV1) Value(index int) []byte {
	if index != 0 {
		panic("index != 0")
	}
	cf, ok := b[columnFamily]
	if !ok || len(cf) != 1 {
		panic("bad response from bigtable")
	}
	return cf[0].Value
}
