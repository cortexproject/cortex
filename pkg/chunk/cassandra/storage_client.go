package cassandra

import (
	"context"
	"flag"

	"github.com/gocql/gocql"

	"github.com/weaveworks/cortex/pkg/chunk"
)

const (
	maxRowReads = 100
)

// Config for a StorageClient
type Config struct {
	address  string
	keyspace string
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.address, "cassandra.address", "", "Address of Cassandra instances.")
	f.StringVar(&cfg.keyspace, "cassandra.keyspace", "", "Keyspace to use in Cassandra.")
}

func (cfg *Config) cluster() *gocql.ClusterConfig {
	cluster := gocql.NewCluster(cfg.address)
	cluster.Keyspace = cfg.keyspace
	cluster.Consistency = gocql.Quorum
	cluster.BatchObserver = observer{}
	cluster.QueryObserver = observer{}
	return cluster
}

// storageClient implements chunk.storageClient for GCP.
type storageClient struct {
	cfg       Config
	schemaCfg chunk.SchemaConfig
	session   *gocql.Session
}

// NewStorageClient returns a new StorageClient.
func NewStorageClient(cfg Config, schemaCfg chunk.SchemaConfig) (chunk.StorageClient, error) {
	session, err := cfg.cluster().CreateSession()
	if err != nil {
		return nil, err
	}

	return &storageClient{
		cfg:       cfg,
		schemaCfg: schemaCfg,
		session:   session,
	}, nil
}

func (s *storageClient) Close() {
	s.session.Close()
}

type writeBatch struct {
	b *gocql.Batch
}

func (s *storageClient) NewWriteBatch() chunk.WriteBatch {
	return writeBatch{
		b: gocql.NewBatch(gocql.UnloggedBatch),
	}
}

func (b writeBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	b.b.Query("INSERT INTO ? (hash, range, value) VALUES (?, ?, ?)",
		tableName, hashValue, rangeValue, value)
}

func (s *storageClient) BatchWrite(ctx context.Context, batch chunk.WriteBatch) error {
	cassandraBatch := batch.(writeBatch)
	return s.session.ExecuteBatch(cassandraBatch.b.WithContext(ctx))
}

func (s *storageClient) QueryPages(ctx context.Context, query chunk.IndexQuery, callback func(result chunk.ReadBatch, lastPage bool) (shouldContinue bool)) error {
	var q *gocql.Query
	if len(query.RangeValuePrefix) > 0 {
		q = s.session.Query("SELECT range, value FROM ? WHERE hash = ? AND range >= ? ",
			query.TableName, query.HashValue, query.RangeValuePrefix)
	} else if len(query.RangeValueStart) > 0 {
		q = s.session.Query("SELECT range, value FROM ? WHERE hash = ? AND range >= ? ",
			query.TableName, query.HashValue, query.RangeValueStart)
	} else {
		q = s.session.Query("SELECT range, value FROM ? WHERE hash = ?",
			query.TableName, query.HashValue)
	}

	iter := q.WithContext(ctx).Iter()
	defer iter.Close()
	scanner := iter.Scanner()
	for scanner.Next() {
		var b readBatch
		if err := scanner.Scan(&b.rangeValue, &b.value); err != nil {
			return err
		}
		if callback(b, false) {
			return nil
		}
	}
	return scanner.Err()
}

// readBatch represents a batch of rows read from Cassandre.
type readBatch struct {
	rangeValue []byte
	value      []byte
}

func (readBatch) Len() int {
	return 1
}

func (b readBatch) RangeValue(index int) []byte {
	if index != 0 {
		panic("index != 0")
	}
	return b.rangeValue
}

func (b readBatch) Value(index int) []byte {
	if index != 0 {
		panic("index != 0")
	}
	return b.value
}

func (s *storageClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	b := gocql.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	for i := range chunks {
		// Encode the chunk first - checksum is calculated as a side effect.
		buf, err := chunks[i].Encode()
		if err != nil {
			return err
		}
		key := chunks[i].ExternalKey()
		tableName := s.schemaCfg.ChunkTables.TableFor(chunks[i].From)
		b.Query("INSERT INTO ? (key, value) VALUES (?, ?)", tableName, key, buf)
	}

	return s.session.ExecuteBatch(b)
}

func (s *storageClient) GetChunks(ctx context.Context, input []chunk.Chunk) ([]chunk.Chunk, error) {
	outs := make(chan chunk.Chunk, len(input))
	errs := make(chan error, len(input))

	for i := 0; i < len(input); i++ {
		c := input[i]
		go func(c chunk.Chunk) {
			out, err := s.getChunk(ctx, c)
			if err != nil {
				errs <- err
			} else {
				outs <- out
			}
		}(c)
	}

	output := make([]chunk.Chunk, 0, len(input))
	for i := 0; i < len(input); i++ {
		select {
		case c := <-outs:
			output = append(output, c)
		case err := <-errs:
			return nil, err
		}
	}

	return output, nil
}

func (s *storageClient) getChunk(ctx context.Context, input chunk.Chunk) (chunk.Chunk, error) {
	tableName := s.schemaCfg.ChunkTables.TableFor(input.From)
	var buf []byte
	if err := s.session.Query("SELECT value FROM ? WHERE key = ?", tableName, input.ExternalKey()).
		WithContext(ctx).Scan(&buf); err != nil {
		return input, err
	}
	decodeContext := chunk.NewDecodeContext()
	err := input.Decode(decodeContext, buf)
	return input, err
}
