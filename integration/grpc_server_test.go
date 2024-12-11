//go:build requires_docker
// +build requires_docker

package integration

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/distributor/distributorpb"
	ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

type mockGprcServer struct {
	ingester_client.IngesterServer
}

func (m mockGprcServer) QueryStream(_ *ingester_client.QueryRequest, streamServer ingester_client.Ingester_QueryStreamServer) error {
	md, _ := metadata.FromIncomingContext(streamServer.Context())
	i, _ := strconv.Atoi(md["i"][0])
	return streamServer.Send(createStreamResponse(i))
}

func (m mockGprcServer) Push(ctx context.Context, request *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	time.Sleep(time.Duration(rand.Int31n(100)) * time.Millisecond)
	md, _ := metadata.FromIncomingContext(ctx)
	i, _ := strconv.Atoi(md["i"][0])
	expected := createRequest(i)

	if expected.String() != request.String() {
		return nil, fmt.Errorf("expected %v, got %v", expected, request)
	}
	return nil, nil
}

func run(t *testing.T, cfg server.Config, register func(s *grpc.Server), validate func(t *testing.T, con *grpc.ClientConn)) {
	savedRegistry := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	defer func() {
		prometheus.DefaultRegisterer = savedRegistry
	}()

	grpcPort, closeGrpcPort, err := getLocalHostPort()
	require.NoError(t, err)
	httpPort, closeHTTPPort, err := getLocalHostPort()
	require.NoError(t, err)

	err = closeGrpcPort()
	require.NoError(t, err)
	err = closeHTTPPort()
	require.NoError(t, err)

	cfg.HTTPListenPort = httpPort
	cfg.GRPCListenPort = grpcPort

	serv, err := server.New(cfg)
	require.NoError(t, err)
	register(serv.GRPC)

	go func() {
		err := serv.Run()
		require.NoError(t, err)
	}()

	defer serv.Shutdown()

	grpcHost := fmt.Sprintf("localhost:%d", grpcPort)

	clientConfig := grpcclient.Config{}
	clientConfig.RegisterFlags(flag.NewFlagSet("fake", flag.ContinueOnError))

	dialOptions, err := clientConfig.DialOption(nil, nil)
	assert.NoError(t, err)
	dialOptions = append([]grpc.DialOption{grpc.WithDefaultCallOptions(clientConfig.CallOptions()...)}, dialOptions...)

	conn, err := grpc.NewClient(grpcHost, dialOptions...)
	assert.NoError(t, err)
	validate(t, conn)
}

func TestConcurrentGrpcCalls(t *testing.T) {
	cfg := server.Config{}
	(&cfg).RegisterFlags(flag.NewFlagSet("fake", flag.ContinueOnError))

	tc := map[string]struct {
		cfg      server.Config
		register func(s *grpc.Server)
		validate func(t *testing.T, con *grpc.ClientConn)
	}{
		"distributor": {
			cfg: cfg,
			register: func(s *grpc.Server) {
				d := &mockGprcServer{}
				distributorpb.RegisterDistributorServer(s, d)
			},
			validate: func(t *testing.T, conn *grpc.ClientConn) {
				client := distributorpb.NewDistributorClient(conn)
				wg := sync.WaitGroup{}
				n := 10000
				wg.Add(n)
				for i := 0; i < n; i++ {
					go func(i int) {
						defer wg.Done()
						ctx := context.Background()
						ctx = metadata.NewOutgoingContext(ctx, metadata.MD{"i": []string{strconv.Itoa(i)}})
						_, err := client.Push(ctx, createRequest(i))
						require.NoError(t, err)
					}(i)
				}

				wg.Wait()
			},
		},
		"ingester": {
			cfg: cfg,
			register: func(s *grpc.Server) {
				d := &mockGprcServer{}
				ingester_client.RegisterIngesterServer(s, d)
			},
			validate: func(t *testing.T, conn *grpc.ClientConn) {
				client := ingester_client.NewIngesterClient(conn)
				wg := sync.WaitGroup{}
				n := 10000
				wg.Add(n)
				for i := 0; i < n; i++ {
					go func(i int) {
						defer wg.Done()
						ctx := context.Background()
						ctx = metadata.NewOutgoingContext(ctx, metadata.MD{"i": []string{strconv.Itoa(i)}})
						s, err := client.QueryStream(ctx, &ingester_client.QueryRequest{})
						require.NoError(t, err)
						resp, err := s.Recv()
						require.NoError(t, err)
						expected := createStreamResponse(i)
						require.Equal(t, expected.String(), resp.String())
					}(i)
				}

				wg.Wait()
			},
		},
	}

	for name, c := range tc {
		t.Run(name, func(t *testing.T) {
			run(t, c.cfg, c.register, c.validate)
		})
	}
}

func createStreamResponse(i int) *ingester_client.QueryStreamResponse {
	return &ingester_client.QueryStreamResponse{Chunkseries: []ingester_client.TimeSeriesChunk{
		{
			FromIngesterId: strconv.Itoa(i),
			Labels:         createLabels(i),
			Chunks: []ingester_client.Chunk{
				{
					StartTimestampMs: int64(i),
					EndTimestampMs:   int64(i),
					Encoding:         int32(i),
					Data:             []byte(strconv.Itoa(i)),
				},
			},
		},
	}}
}

func createRequest(i int) *cortexpb.WriteRequest {
	labels := createLabels(i)
	return &cortexpb.WriteRequest{
		Timeseries: []cortexpb.PreallocTimeseries{
			{
				TimeSeries: &cortexpb.TimeSeries{
					Labels: labels,
					Samples: []cortexpb.Sample{
						{TimestampMs: int64(i), Value: float64(i)},
					},
					Exemplars: []cortexpb.Exemplar{
						{
							Labels:      labels,
							Value:       float64(i),
							TimestampMs: int64(i),
						},
					},
				},
			},
		},
	}
}

func createLabels(i int) []cortexpb.LabelPair {
	labels := make([]cortexpb.LabelPair, 0, 100)
	for j := 0; j < 100; j++ {
		labels = append(labels, cortexpb.LabelPair{
			Name:  fmt.Sprintf("test%d_%d", i, j),
			Value: fmt.Sprintf("test%d_%d", i, j),
		})
	}
	return labels
}

func getLocalHostPort() (int, func() error, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, nil, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, nil, err
	}

	closePort := func() error {
		return l.Close()
	}
	return l.Addr().(*net.TCPAddr).Port, closePort, nil
}
