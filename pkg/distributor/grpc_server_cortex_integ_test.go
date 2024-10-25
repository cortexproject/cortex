package distributor

import (
	"context"
	"flag"
	"fmt"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/distributor/distributorpb"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
)

type mockDistributor struct {
}

func (m mockDistributor) Push(ctx context.Context, request *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	time.Sleep(time.Duration(rand.Int31n(100)) * time.Millisecond)
	if request.Timeseries[0].Labels[0].Name != request.Timeseries[0].Labels[0].Value {
		return nil, fmt.Errorf("expected label %v, got %v", request.Timeseries[0].Labels[0].Name, request.Timeseries[0].Labels[0].Value)
	}
	return nil, nil
}

func TestGrpcSerialization(t *testing.T) {
	cfg := server.Config{}
	(&cfg).RegisterFlags(flag.NewFlagSet("fake", flag.ContinueOnError))
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

	d := &mockDistributor{}
	distributorpb.RegisterDistributorServer(serv.GRPC, d)

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

	client := distributorpb.NewDistributorClient(conn)
	wg := sync.WaitGroup{}
	n := 10000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, err := client.Push(context.Background(), &cortexpb.WriteRequest{
				Timeseries: []cortexpb.PreallocTimeseries{
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: []cortexpb.LabelAdapter{
								{
									Name:  strings.Repeat(fmt.Sprintf("test%d", i), 1000),
									Value: strings.Repeat(fmt.Sprintf("test%d", i), 1000),
								},
							},
						},
					},
				},
			})

			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

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
