package ingester

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
)

func BenchmarkQueryStream(b *testing.B) {
	cfg := defaultIngesterTestConfig()
	clientCfg := defaultClientTestConfig()
	limits := defaultLimitsTestConfig()

	const (
		numSeries  = 1e6 // Put 1 million timeseries, each with 100 samples.
		numSamples = 100
		numCPUs    = 32
	)

	encoding.DefaultEncoding = encoding.Bigchunk
	limits.MaxSeriesPerMetric = numSeries
	limits.MaxSeriesPerQuery = numSeries
	cfg.FlushCheckPeriod = 15 * time.Minute
	_, ing := newTestStore(b, cfg, clientCfg, limits)
	// defer ing.Shutdown()

	ctx := user.InjectOrgID(context.Background(), "1")
	instances := make([]string, numSeries/numCPUs)
	for i := 0; i < numSeries/numCPUs; i++ {
		instances[i] = fmt.Sprintf("node%04d", i)
	}
	cpus := make([]string, numCPUs)
	for i := 0; i < numCPUs; i++ {
		cpus[i] = fmt.Sprintf("cpu%02d", i)
	}

	for i := 0; i < numSeries; i++ {
		labels := labelPairs{
			{Name: model.MetricNameLabel, Value: "node_cpu"},
			{Name: "job", Value: "node_exporter"},
			{Name: "instance", Value: instances[i/numCPUs]},
			{Name: "cpu", Value: cpus[i%numCPUs]},
		}

		state, fp, series, err := ing.userStates.getOrCreateSeries(ctx, labels)
		require.NoError(b, err)

		for j := 0; j < numSamples; j++ {
			err = series.add(model.SamplePair{
				Value:     model.SampleValue(float64(i)),
				Timestamp: model.Time(int64(i)),
			})
			require.NoError(b, err)
		}

		state.fpLocker.Unlock(fp)
	}

	server := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer server.GracefulStop()
	client.RegisterIngesterServer(server, ing)

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)
	go server.Serve(l)

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		b.Run("QueryStream", func(b *testing.B) {
			c, err := client.MakeIngesterClient(l.Addr().String(), clientCfg)
			require.NoError(b, err)
			defer c.Close()

			s, err := c.QueryStream(ctx, &client.QueryRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   numSamples,
				Matchers: []*client.LabelMatcher{{
					Type:  client.EQUAL,
					Name:  model.MetricNameLabel,
					Value: "node_cpu",
				}},
			})
			require.NoError(b, err)

			count := 0
			for {
				resp, err := s.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(b, err)
				count += len(resp.Timeseries)
			}
			require.Equal(b, count, int(numSeries))
		})
	}
}
