package grpc

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/pkg/errors"

	"google.golang.org/grpc"

	"google.golang.org/grpc/keepalive"

	"google.golang.org/grpc/reflection"

	"github.com/cortexproject/cortex/pkg/chunk"

	"github.com/golang/protobuf/ptypes/empty"
)

type server struct {
	Cfg Config `yaml:"cfg,omitempty"`
}

// indexClient RPCs
func (s server) BatchWrite(ctx context.Context, writes *BatchWrites) (*empty.Empty, error) {
	rangeValue := "JSI0YbyRLVmLKkLBiAKf5ctf8mWtn9U6CXCzuYmWkMk 5f3DoSEa2cDzymQ7u8VZ6c/ku1HlYIdMWqdg1QKCYh4  8"
	value := "localhost:9090"
	if writes.Writes[0].TableName == "index_2625" &&
		writes.Writes[0].HashValue == "fake:d18381:5f3DoSEa2cDzymQ7u8VZ6c/ku1HlYIdMWqdg1QKCYh4" && string(writes.Writes[0].RangeValue) == rangeValue &&
		string(writes.Writes[0].Value) == value {
		return &empty.Empty{}, nil
	}
	err := errors.New("batch write request from indexClient doesn't match with the gRPC client")
	return &empty.Empty{}, err
}

func (s server) QueryPages(query *IndexQuery, pagesServer GrpcStore_QueryPagesServer) error {
	if query.TableName == "table" && query.HashValue == "foo" {
		return nil
	}
	err := errors.New("query pages from indexClient request doesn't match with the gRPC client")
	return err
}

func (s server) Delete(ctx context.Context, deletes *BatchDeletes) (*empty.Empty, error) {
	if deletes.Deletes[0].TableName == "index_2625" && deletes.Deletes[0].HashValue == "fake:d18381:5f3DoSEa2cDzymQ7u8VZ6c/ku1HlYIdMWqdg1QKCYh4" &&
		string(deletes.Deletes[0].RangeValue) == "JSI0YbyRLVmLKkLBiAKf5ctf8mWtn9U6CXCzuYmWkMk 5f3DoSEa2cDzymQ7u8VZ6c/ku1HlYIdMWqdg1QKCYh4  8" {
		return &empty.Empty{}, nil
	}
	err := errors.New("delete from indexClient request doesn't match with the gRPC client")
	return &empty.Empty{}, err
}

// storageClient RPCs
func (s server) PutChunks(ctx context.Context, request *PutChunksRequest) (*empty.Empty, error) {
	//encoded :=
	if request.Chunks[0].TableName == "" && request.Chunks[0].Key == "fake/ddf337b84e835f32:171bc00155a:171bc00155a:fc8fd207" {
		return &empty.Empty{}, nil
	}
	err := errors.New("putChunks from storageClient request doesn't match with test from gRPC client")
	return &empty.Empty{}, err
}

func (s server) GetChunks(request *GetChunksRequest, chunksServer GrpcStore_GetChunksServer) error {
	if request.Chunks[0].TableName == "" && request.Chunks[0].Key == "fake/ddf337b84e835f32:171bc00155a:171bc00155a:d9a103b5" &&
		request.Chunks[0].Encoded == nil {
		return nil
	}
	err := errors.New("getChunks from storageClient request doesn't match with test gRPC client")
	return err
}

func (s server) DeleteChunks(ctx context.Context, id *ChunkID) (*empty.Empty, error) {
	if id.ChunkID == "" {
		return &empty.Empty{}, nil
	}
	err := errors.New("deleteChunks from storageClient request doesn't match with test gRPC client")
	return &empty.Empty{}, err
}

// tableClient RPCs
func (s server) ListTables(ctx context.Context, empty *empty.Empty) (*ListTablesResponse, error) {
	return &ListTablesResponse{
		TableNames: []string{"chunk_2604, chunk_2613, index_2594, index_2603"},
	}, nil
}

func (s server) CreateTable(ctx context.Context, desc *TableDesc) (*empty.Empty, error) {
	if desc.Name == "chunk_2607" && !desc.UseOnDemandIOMode && desc.ProvisionedRead == 300 && desc.ProvisionedWrite == 1 && desc.Tags == nil {
		return &empty.Empty{}, nil
	}
	err := errors.New("create table from tableClient request doesn't match with test gRPC client")
	return &empty.Empty{}, err
}

func (s server) DeleteTable(ctx context.Context, name *TableName) (*empty.Empty, error) {
	if name.TableName == "chunk_2591" {
		return &empty.Empty{}, nil
	}
	err := errors.New("delete table from tableClient request doesn't match with test gRPC client")
	return &empty.Empty{}, err
}

func (s server) DescribeTable(ctx context.Context, name *TableName) (*DescribeTableResponse, error) {
	if name.TableName == "chunk_2591" {
		return &DescribeTableResponse{
			Desc: &TableDesc{
				Name:              "chunk_2591",
				UseOnDemandIOMode: false,
				ProvisionedRead:   0,
				ProvisionedWrite:  0,
				Tags:              nil,
			},
			IsActive: true,
		}, nil
	}
	err := errors.New("describe table from tableClient request doesn't match with test gRPC client")
	return &DescribeTableResponse{}, err
}

func (s server) UpdateTable(ctx context.Context, request *UpdateTableRequest) (*empty.Empty, error) {
	if request.Current.Name == "chunk_2591" && !request.Current.UseOnDemandIOMode && request.Current.ProvisionedWrite == 0 &&
		request.Current.ProvisionedRead == 0 && request.Current.Tags == nil && request.Expected.Name == "chunk_2591" &&
		!request.Expected.UseOnDemandIOMode && request.Expected.ProvisionedWrite == 1 &&
		request.Expected.ProvisionedRead == 300 && request.Expected.Tags == nil {
		return &empty.Empty{}, nil
	}
	err := errors.New("update table from tableClient request doesn't match with test gRPC client")
	return &empty.Empty{}, err
}

func (s server) Stop(ctx context.Context, empty *empty.Empty) (*empty.Empty, error) {
	return empty, nil
}

// NewStorageClient returns a new StorageClient.
func NewTestStorageClient(cfg Config, schemaCfg chunk.SchemaConfig) (*StorageClient, error) {
	grpcClient, _, err := connectToTestGrpcServer(cfg.Address)
	if err != nil {
		return nil, err
	}
	client := &StorageClient{
		schemaCfg: schemaCfg,
		client:    grpcClient,
	}
	return client, nil
}

//***********************  gRPC mock server *********************************//

// NewTableClient returns a new TableClient.
func NewTestTableClient(cfg Config) (*TableClient, error) {
	grpcClient, _, err := connectToTestGrpcServer(cfg.Address)
	if err != nil {
		return nil, err
	}
	client := &TableClient{
		client: grpcClient,
	}
	return client, nil
}

// NewStorageClient returns a new StorageClient.
func newTestStorageServer(cfg Config) (*server, error) {
	client := &server{
		Cfg: cfg,
	}
	return client, nil
}

func connectToTestGrpcServer(serverAddress string) (GrpcStoreClient, *grpc.ClientConn, error) {
	params := keepalive.ClientParameters{
		Time:                time.Second * 20,
		Timeout:             time.Minute * 10,
		PermitWithoutStream: true,
	}
	param := grpc.WithKeepaliveParams(params)
	cc, err := grpc.Dial(serverAddress, param, grpc.WithInsecure())
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to dial grpc-store %s", serverAddress)
	}
	return NewGrpcStoreClient(cc), cc, nil
}

func createTestGrpcServer() {
	var cfg server
	lis, err := net.Listen("tcp", "localhost:6688")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	// Enable reflection to expose endpoints this server offers.
	reflection.Register(s)

	s1, err := newTestStorageServer(cfg.Cfg)
	if err != nil {
		log.Fatalf("Failed to created new storage client")
	}
	RegisterGrpcStoreServer(s, s1)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()
}
