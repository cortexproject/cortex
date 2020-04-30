package grpc

import (
	"context"
	"io"

	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/util"
)

type StorageClient struct {
	schemaCfg chunk.SchemaConfig
	client    GrpcStoreClient
}

// NewStorageClient returns a new StorageClient.
func NewStorageClient(cfg Config, schemaCfg chunk.SchemaConfig) (*StorageClient, error) {
	grpcClient, _, err := connectToGrpcServer(cfg.Address)
	if err != nil {
		return nil, err
	}
	client := &StorageClient{
		schemaCfg: schemaCfg,
		client:    grpcClient,
	}
	return client, nil
}

func (s *StorageClient) Stop() {
	_, err := s.client.Stop(context.Background(), &empty.Empty{})
	if err != nil {
		level.Error(util.Logger).Log("failed to the stop the storage client connection", err)
	}
}

// PutChunks implements chunk.ObjectClient.
func (s *StorageClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	chunksInfo := &PutChunksRequest{}
	for i := range chunks {
		buf, err := chunks[i].Encoded()
		if err != nil {
			return errors.WithStack(err)
		}

		key := chunks[i].ExternalKey()
		tableName, err := s.schemaCfg.ChunkTableFor(chunks[i].From)
		if err != nil {
			return errors.WithStack(err)
		}
		writeChunk := &Chunk{
			Encoded:   buf,
			Key:       key,
			TableName: tableName,
		}

		chunksInfo.Chunks = append(chunksInfo.Chunks, writeChunk)
	}

	_, err := s.client.PutChunks(ctx, chunksInfo)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *StorageClient) DeleteChunk(ctx context.Context, chunkID string) error {
	chunkInfo := &ChunkID{ChunkID: chunkID}
	_, err := s.client.DeleteChunks(ctx, chunkInfo)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *StorageClient) GetChunks(ctx context.Context, input []chunk.Chunk) ([]chunk.Chunk, error) {
	chunks := &GetChunksRequest{}
	chunks.Chunks = []*Chunk{}
	var err error
	for _, inputInfo := range input {
		chunkInfo := &Chunk{}
		// send the table name from upstream gRPC client as gRPC server is unaware of schema
		chunkInfo.TableName, err = s.schemaCfg.ChunkTableFor(inputInfo.From)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		chunkInfo.Key = inputInfo.ExternalKey()
		chunks.Chunks = append(chunks.Chunks, chunkInfo)
	}
	streamer, err := s.client.GetChunks(ctx, chunks)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var result []chunk.Chunk
	var decodeContext *chunk.DecodeContext
	for {
		receivedChunks, err := streamer.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
		for _, chunkResponse := range receivedChunks.GetChunks() {
			var c chunk.Chunk
			if chunkResponse != nil {
				decodeContext = chunk.NewDecodeContext()
				err = c.Decode(decodeContext, chunkResponse.Encoded)
				if err != nil {
					return result, err
				}
			}
			if c.Data != nil {
				result = append(result, c)
			}
		}
	}

	return result, err
}
