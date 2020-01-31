package main

import (
	"context"
	"log"
	"net"
	"sync"
	"testing"

	proto12 "github.com/cortexproject/cortex/integration-tests/proto/1.2.1"
	proto13 "github.com/cortexproject/cortex/integration-tests/proto/1.3.0"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var (
	lis         *bufconn.Listener
	server      *grpc.Server
	labelNames  = []string{"foo", "fuzz", "bar", "buzz"}
	labelValues = []string{"lfoo", "lfuzz", "lbar", "lbuzz"}
	wg          = sync.WaitGroup{}
)

func init() {
	lis = bufconn.Listen(bufSize)
	server = grpc.NewServer()
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func Test_ProtoCompat(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	// 1.3.0 server
	proto13.RegisterIngesterServer(server, &fakeIngesterServer{t})
	// 1.2.1 client
	client := proto12.NewIngesterClient(conn)

	go func() {
		if err := server.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	l, err := client.LabelNames(ctx, &proto12.LabelNamesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, labelNames, l.LabelNames)

	v, err := client.LabelValues(ctx, &proto12.LabelValuesRequest{LabelName: "foo"})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, labelValues, v.LabelValues)

	s, err := client.UserStats(ctx, &proto12.UserStatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, &proto12.UserStatsResponse{
		IngestionRate:     10.5,
		NumSeries:         1000000,
		ApiIngestionRate:  20.245,
		RuleIngestionRate: 39.033,
	}, s)

	r, err := client.Push(ctx, &proto12.WriteRequest{
		Source: proto12.RULE,
		Timeseries: []proto12.PreallocTimeseries{
			proto12.PreallocTimeseries{
				TimeSeries: &proto12.TimeSeries{
					Labels: []proto12.LabelAdapter{
						proto12.LabelAdapter{Name: "foo", Value: "bar"},
						proto12.LabelAdapter{Name: "fuzz", Value: "buzz"},
					},
					Samples: []proto12.Sample{
						proto12.Sample{Value: 10, TimestampMs: 1},
						proto12.Sample{Value: 10000, TimestampMs: 2},
					},
				},
			},
			proto12.PreallocTimeseries{
				TimeSeries: &proto12.TimeSeries{
					Labels: []proto12.LabelAdapter{
						proto12.LabelAdapter{Name: "foo", Value: "buzz"},
						proto12.LabelAdapter{Name: "fuzz", Value: "bar"},
					},
					Samples: []proto12.Sample{
						proto12.Sample{Value: 20.1234, TimestampMs: 1},
						proto12.Sample{Value: 25.1233, TimestampMs: 2},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	require.NotNil(t, r)
	q, err := client.Query(ctx, &proto12.QueryRequest{
		StartTimestampMs: 1111,
		EndTimestampMs:   20000,
		Matchers: []*proto12.LabelMatcher{
			&proto12.LabelMatcher{Type: proto12.REGEX_MATCH, Name: "foo", Value: "bar"},
			&proto12.LabelMatcher{Type: proto12.NOT_EQUAL, Name: "fuzz", Value: "buzz"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, &proto12.QueryResponse{
		Timeseries: []proto12.TimeSeries{
			proto12.TimeSeries{
				Labels: []proto12.LabelAdapter{
					proto12.LabelAdapter{Name: "foo", Value: "bar"},
					proto12.LabelAdapter{Name: "fuzz", Value: "buzz"},
				},
				Samples: []proto12.Sample{
					proto12.Sample{Value: 10, TimestampMs: 1},
					proto12.Sample{Value: 10000, TimestampMs: 2},
				},
			},
		},
	}, q)

	stream, err := client.QueryStream(ctx, &proto12.QueryRequest{
		StartTimestampMs: 1111,
		EndTimestampMs:   20000,
		Matchers: []*proto12.LabelMatcher{
			&proto12.LabelMatcher{Type: proto12.REGEX_MATCH, Name: "foo", Value: "bar"},
			&proto12.LabelMatcher{Type: proto12.NOT_EQUAL, Name: "fuzz", Value: "buzz"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	streamResponse, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, &proto12.QueryStreamResponse{
		Timeseries: []proto12.TimeSeriesChunk{
			proto12.TimeSeriesChunk{
				FromIngesterId: "1",
				UserId:         "2",
				Labels: []proto12.LabelAdapter{
					proto12.LabelAdapter{Name: "foo", Value: "barr"},
				},
				Chunks: []proto12.Chunk{
					proto12.Chunk{
						StartTimestampMs: 1,
						EndTimestampMs:   1000,
						Encoding:         5,
						Data:             []byte{1, 1, 3, 5, 12},
					},
				},
			},
		},
	}, streamResponse)
	stats, err := client.AllUserStats(ctx, &proto12.UserStatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, &proto12.UsersStatsResponse{
		Stats: []*proto12.UserIDStatsResponse{
			&proto12.UserIDStatsResponse{
				UserId: "fake",
				Data: &proto12.UserStatsResponse{
					IngestionRate:     1,
					NumSeries:         2,
					ApiIngestionRate:  2,
					RuleIngestionRate: 1.223,
				},
			},
		},
	}, stats)

	m, err := client.MetricsForLabelMatchers(ctx, &proto12.MetricsForLabelMatchersRequest{
		StartTimestampMs: 1,
		EndTimestampMs:   2,
		MatchersSet: []*proto12.LabelMatchers{
			&proto12.LabelMatchers{
				Matchers: []*proto12.LabelMatcher{
					&proto12.LabelMatcher{
						Type:  proto12.REGEX_MATCH,
						Name:  "foo",
						Value: "buzz",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, &proto12.MetricsForLabelMatchersResponse{
		Metric: []*proto12.Metric{
			&proto12.Metric{Labels: []proto12.LabelAdapter{
				proto12.LabelAdapter{Name: "foo", Value: "bar"},
			}},
		},
	}, m)

	chunkStream, err := client.TransferChunks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	err = chunkStream.Send(&proto12.TimeSeriesChunk{
		FromIngesterId: "foo",
		UserId:         "fake",
		Labels: []proto12.LabelAdapter{
			proto12.LabelAdapter{Name: "bar", Value: "buzz"},
		},
		Chunks: []proto12.Chunk{
			proto12.Chunk{
				StartTimestampMs: 1,
				EndTimestampMs:   2,
				Encoding:         3,
				Data:             []byte{1, 23, 24, 43, 4, 123},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	err = stream.CloseSend()
	require.NoError(t, err)
	tsStream, err := client.TransferTSDB(ctx)
	require.NoError(t, err)
	wg.Add(1)
	err = tsStream.Send(&proto12.TimeSeriesFile{
		FromIngesterId: "string",
		UserId:         "fake",
		Filename:       "here",
		Data:           []byte{1, 2, 3, 5, 6, 7},
	})
	require.NoError(t, err)
	wg.Wait()
	require.NoError(t, tsStream.CloseSend())
}

type fakeIngesterServer struct {
	t *testing.T
}

func (f *fakeIngesterServer) LabelValues(_ context.Context, v *proto13.LabelValuesRequest) (*proto13.LabelValuesResponse, error) {
	require.Equal(f.t, "foo", v.LabelName)
	return &proto13.LabelValuesResponse{
		LabelValues: labelValues,
	}, nil
}
func (fakeIngesterServer) LabelNames(context.Context, *proto13.LabelNamesRequest) (*proto13.LabelNamesResponse, error) {
	return &proto13.LabelNamesResponse{
		LabelNames: labelNames,
	}, nil
}
func (fakeIngesterServer) UserStats(context.Context, *proto13.UserStatsRequest) (*proto13.UserStatsResponse, error) {
	return &proto13.UserStatsResponse{
		IngestionRate:     10.5,
		NumSeries:         1000000,
		ApiIngestionRate:  20.245,
		RuleIngestionRate: 39.033,
	}, nil
}
func (f *fakeIngesterServer) Push(_ context.Context, req *proto13.WriteRequest) (*proto13.WriteResponse, error) {
	require.Equal(f.t, &proto13.WriteRequest{
		Source: proto13.RULE,
		Timeseries: []proto13.PreallocTimeseries{
			proto13.PreallocTimeseries{
				TimeSeries: &proto13.TimeSeries{
					Labels: []proto13.LabelAdapter{
						proto13.LabelAdapter{Name: "foo", Value: "bar"},
						proto13.LabelAdapter{Name: "fuzz", Value: "buzz"},
					},
					Samples: []proto13.Sample{
						proto13.Sample{Value: 10, TimestampMs: 1},
						proto13.Sample{Value: 10000, TimestampMs: 2},
					},
				},
			},
			proto13.PreallocTimeseries{
				TimeSeries: &proto13.TimeSeries{
					Labels: []proto13.LabelAdapter{
						proto13.LabelAdapter{Name: "foo", Value: "buzz"},
						proto13.LabelAdapter{Name: "fuzz", Value: "bar"},
					},
					Samples: []proto13.Sample{
						proto13.Sample{Value: 20.1234, TimestampMs: 1},
						proto13.Sample{Value: 25.1233, TimestampMs: 2},
					},
				},
			},
		},
	}, req)
	return &proto13.WriteResponse{}, nil
}
func (f *fakeIngesterServer) Query(_ context.Context, req *proto13.QueryRequest) (*proto13.QueryResponse, error) {
	require.Equal(f.t, &proto13.QueryRequest{
		StartTimestampMs: 1111,
		EndTimestampMs:   20000,
		Matchers: []*proto13.LabelMatcher{
			&proto13.LabelMatcher{Type: proto13.REGEX_MATCH, Name: "foo", Value: "bar"},
			&proto13.LabelMatcher{Type: proto13.NOT_EQUAL, Name: "fuzz", Value: "buzz"},
		},
	}, req)
	return &proto13.QueryResponse{
		Timeseries: []proto13.TimeSeries{
			proto13.TimeSeries{
				Labels: []proto13.LabelAdapter{
					proto13.LabelAdapter{Name: "foo", Value: "bar"},
					proto13.LabelAdapter{Name: "fuzz", Value: "buzz"},
				},
				Samples: []proto13.Sample{
					proto13.Sample{Value: 10, TimestampMs: 1},
					proto13.Sample{Value: 10000, TimestampMs: 2},
				},
			},
		},
	}, nil
}
func (f *fakeIngesterServer) QueryStream(req *proto13.QueryRequest, stream proto13.Ingester_QueryStreamServer) error {
	require.Equal(f.t, &proto13.QueryRequest{
		StartTimestampMs: 1111,
		EndTimestampMs:   20000,
		Matchers: []*proto13.LabelMatcher{
			&proto13.LabelMatcher{Type: proto13.REGEX_MATCH, Name: "foo", Value: "bar"},
			&proto13.LabelMatcher{Type: proto13.NOT_EQUAL, Name: "fuzz", Value: "buzz"},
		},
	}, req)
	stream.Send(&proto13.QueryStreamResponse{
		Timeseries: []proto13.TimeSeriesChunk{
			proto13.TimeSeriesChunk{
				FromIngesterId: "1",
				UserId:         "2",
				Labels: []proto13.LabelAdapter{
					proto13.LabelAdapter{Name: "foo", Value: "barr"},
				},
				Chunks: []proto13.Chunk{
					proto13.Chunk{
						StartTimestampMs: 1,
						EndTimestampMs:   1000,
						Encoding:         5,
						Data:             []byte{1, 1, 3, 5, 12},
					},
				},
			},
		},
	})
	return nil
}
func (fakeIngesterServer) AllUserStats(context.Context, *proto13.UserStatsRequest) (*proto13.UsersStatsResponse, error) {
	return &proto13.UsersStatsResponse{
		Stats: []*proto13.UserIDStatsResponse{
			&proto13.UserIDStatsResponse{
				UserId: "fake",
				Data: &proto13.UserStatsResponse{
					IngestionRate:     1,
					NumSeries:         2,
					ApiIngestionRate:  2,
					RuleIngestionRate: 1.223,
				},
			},
		},
	}, nil
}
func (f *fakeIngesterServer) MetricsForLabelMatchers(_ context.Context, req *proto13.MetricsForLabelMatchersRequest) (*proto13.MetricsForLabelMatchersResponse, error) {
	require.Equal(f.t, &proto13.MetricsForLabelMatchersRequest{
		StartTimestampMs: 1,
		EndTimestampMs:   2,
		MatchersSet: []*proto13.LabelMatchers{
			&proto13.LabelMatchers{
				Matchers: []*proto13.LabelMatcher{
					&proto13.LabelMatcher{
						Type:  proto13.REGEX_MATCH,
						Name:  "foo",
						Value: "buzz",
					},
				},
			},
		},
	}, req)
	return &proto13.MetricsForLabelMatchersResponse{
		Metric: []*proto13.Metric{
			&proto13.Metric{Labels: []proto13.LabelAdapter{
				proto13.LabelAdapter{Name: "foo", Value: "bar"},
			}},
		},
	}, nil
}
func (f *fakeIngesterServer) TransferChunks(s proto13.Ingester_TransferChunksServer) error {
	c, err := s.Recv()
	require.NoError(f.t, err)
	require.Equal(f.t, &proto13.TimeSeriesChunk{
		FromIngesterId: "foo",
		UserId:         "fake",
		Labels: []proto13.LabelAdapter{
			proto13.LabelAdapter{Name: "bar", Value: "buzz"},
		},
		Chunks: []proto13.Chunk{
			proto13.Chunk{
				StartTimestampMs: 1,
				EndTimestampMs:   2,
				Encoding:         3,
				Data:             []byte{1, 23, 24, 43, 4, 123},
			},
		},
	}, c)
	wg.Done()
	return nil
}

// TransferTSDB transfers all files of a tsdb to a joining ingester
func (f *fakeIngesterServer) TransferTSDB(stream proto13.Ingester_TransferTSDBServer) error {
	file, err := stream.Recv()
	require.NoError(f.t, err)
	require.Equal(f.t, &proto13.TimeSeriesFile{
		FromIngesterId: "string",
		UserId:         "fake",
		Filename:       "here",
		Data:           []byte{1, 2, 3, 5, 6, 7},
	}, file)
	wg.Done()
	return nil
}
