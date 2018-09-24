package testutils

import (
	"context"
	"strconv"
	"time"

	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/chunk"
)

// Fixture type for per-backend testing.
type Fixture interface {
	Name() string
	Clients() (chunk.IndexClient, chunk.ObjectClient, chunk.TableClient, chunk.SchemaConfig, error)
	Teardown() error
}

// DefaultSchemaConfig returns default schema for use in test fixtures
func DefaultSchemaConfig(kind string) chunk.SchemaConfig {
	schemaConfig := chunk.DefaultSchemaConfig(kind, "v1", model.Now().Add(-time.Hour*2))
	return schemaConfig
}

// Setup a fixture with initial tables
func Setup(fixture Fixture, tableName string) (chunk.IndexClient, chunk.ObjectClient, error) {
	var tbmConfig chunk.TableManagerConfig
	flagext.DefaultValues(&tbmConfig)
	indexClient, objectClient, tableClient, schemaConfig, err := fixture.Clients()
	if err != nil {
		return nil, nil, err
	}

	tableManager, err := chunk.NewTableManager(tbmConfig, schemaConfig, 12*time.Hour, tableClient)
	if err != nil {
		return nil, nil, err
	}

	err = tableManager.SyncTables(context.Background())
	if err != nil {
		return nil, nil, err
	}

	err = tableClient.CreateTable(context.Background(), chunk.TableDesc{
		Name: tableName,
	})
	return indexClient, objectClient, err
}

// CreateChunkOptions allows options to be passed when creating chunks
type CreateChunkOptions interface {
	set(req *createChunkRequest)
}

type createChunkRequest struct {
	userID string
}

// User applies a new user ID to chunks created by CreateChunks
func User(user string) UserOpt { return UserOpt(user) }

// UserOpt is used to set the user for a set of chunks
type UserOpt string

func (u UserOpt) set(req *createChunkRequest) {
	req.userID = string(u)
}

// CreateChunks creates some chunks for testing
func CreateChunks(startIndex, batchSize int, start model.Time, options ...CreateChunkOptions) ([]string, []chunk.Chunk, error) {
	req := &createChunkRequest{
		userID: "userID",
	}
	for _, opt := range options {
		opt.set(req)
	}
	keys := []string{}
	chunks := []chunk.Chunk{}
	for j := 0; j < batchSize; j++ {
		chunk := dummyChunkFor(start, model.Metric{
			model.MetricNameLabel: "foo",
			"index":               model.LabelValue(strconv.Itoa(startIndex*batchSize + j)),
		}, req.userID)
		chunks = append(chunks, chunk)
		keys = append(keys, chunk.ExternalKey())
	}
	return keys, chunks, nil
}

func dummyChunkFor(now model.Time, metric model.Metric, userID string) chunk.Chunk {
	cs, _ := promchunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})
	chunk := chunk.NewChunk(
		userID,
		metric.Fingerprint(),
		metric,
		cs[0],
		now.Add(-time.Hour),
		now,
	)
	// Force checksum calculation.
	err := chunk.Encode()
	if err != nil {
		panic(err)
	}
	return chunk
}
