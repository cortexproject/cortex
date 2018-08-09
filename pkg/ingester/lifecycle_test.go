package ingester

import (
	"io"
	"math"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	userID    = "1"
	aLongTime = 60 * time.Second
)

func defaultIngesterTestConfig() Config {
	consul := ring.NewInMemoryKVClient()
	cfg := Config{}
	util.DefaultValues(&cfg)
	cfg.FlushCheckPeriod = 99999 * time.Hour
	cfg.MaxChunkIdle = 99999 * time.Hour
	cfg.ConcurrentFlushes = 1
	cfg.LifecyclerConfig.KVClient = consul
	cfg.LifecyclerConfig.NumTokens = 1
	cfg.LifecyclerConfig.ListenPort = func(i int) *int { return &i }(0)
	cfg.LifecyclerConfig.Addr = "localhost"
	cfg.LifecyclerConfig.ID = "localhost"
	return cfg
}

// TestIngesterRestart tests a restarting ingester doesn't keep adding more tokens.
func TestIngesterRestart(t *testing.T) {
	config := defaultIngesterTestConfig()
	config.LifecyclerConfig.SkipUnregister = true

	{
		ingester, err := New(config, nil)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		ingester.Shutdown() // doesn't actually unregister due to skipUnregister: true
	}

	poll(t, 100*time.Millisecond, 1, func() interface{} {
		return numTokens(config.LifecyclerConfig.KVClient, "localhost")
	})

	{
		ingester, err := New(config, nil)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		ingester.Shutdown() // doesn't actually unregister due to skipUnregister: true
	}

	time.Sleep(200 * time.Millisecond)

	poll(t, 100*time.Millisecond, 1, func() interface{} {
		return numTokens(config.LifecyclerConfig.KVClient, "localhost")
	})
}

func TestIngesterTransfer(t *testing.T) {
	cfg := defaultIngesterTestConfig()

	// Start the first ingester, and get it into ACTIVE state.
	cfg1 := cfg
	cfg1.LifecyclerConfig.ID = "ingester1"
	cfg1.LifecyclerConfig.Addr = "ingester1"
	cfg1.LifecyclerConfig.ClaimOnRollout = true
	cfg1.SearchPendingFor = aLongTime
	ing1, err := New(cfg1, nil)
	require.NoError(t, err)

	poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return ing1.lifecycler.GetState()
	})

	// Now write a sample to this ingester
	var (
		ts  = model.TimeFromUnix(123)
		val = model.SampleValue(456)
		m   = model.Metric{
			model.MetricNameLabel: "foo",
		}
	)
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = ing1.Push(ctx, client.ToWriteRequest([]model.Sample{
		{
			Metric:    m,
			Timestamp: ts,
			Value:     val,
		},
	}))
	require.NoError(t, err)

	// Start a second ingester, but let it go into PENDING
	cfg2 := cfg
	cfg2.LifecyclerConfig.ID = "ingester2"
	cfg2.LifecyclerConfig.Addr = "ingester2"
	cfg2.LifecyclerConfig.JoinAfter = aLongTime
	ing2, err := New(cfg2, nil)
	require.NoError(t, err)

	// Let ing2 send chunks to ing1
	ing1.cfg.ingesterClientFactory = func(addr string, _ client.Config) (client.IngesterClient, error) {
		return ingesterClientAdapater{
			ingester: ing2,
		}, nil
	}

	// Now stop the first ingester
	ing1.Shutdown()

	// And check the second ingester has the sample
	matcher, err := labels.NewMatcher(labels.MatchEqual, model.MetricNameLabel, "foo")
	require.NoError(t, err)

	request, err := client.ToQueryRequest(model.TimeFromUnix(0), model.TimeFromUnix(200), []*labels.Matcher{matcher})
	require.NoError(t, err)

	response, err := ing2.Query(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, &client.QueryResponse{
		Timeseries: []client.TimeSeries{
			{
				Labels: client.ToLabelPairs(m),
				Samples: []client.Sample{
					{
						Value:       456,
						TimestampMs: 123000,
					},
				},
			},
		},
	}, response)
}

func numTokens(c ring.KVClient, name string) int {
	ringDesc, err := c.Get(context.Background(), ring.ConsulKey)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error reading consul", "err", err)
		return 0
	}

	count := 0
	for _, token := range ringDesc.(*ring.Desc).Tokens {
		if token.Ingester == name {
			count++
		}
	}
	return count
}

// poll repeatedly evaluates condition until we either timeout, or it succeeds.
func poll(t *testing.T, d time.Duration, want interface{}, have func() interface{}) {
	deadline := time.Now().Add(d)
	for {
		if time.Now().After(deadline) {
			break
		}
		if reflect.DeepEqual(want, have()) {
			return
		}
		time.Sleep(d / 10)
	}
	h := have()
	if !reflect.DeepEqual(want, h) {
		_, file, line, _ := runtime.Caller(1)
		t.Fatalf("%s:%d: %v != %v", file, line, want, h)
	}
}

type ingesterTransferChunkStreamMock struct {
	ctx  context.Context
	reqs chan *client.TimeSeriesChunk
	resp chan *client.TransferChunksResponse
	err  chan error

	grpc.ServerStream
	grpc.ClientStream
}

func (s *ingesterTransferChunkStreamMock) Send(tsc *client.TimeSeriesChunk) error {
	s.reqs <- tsc
	return nil
}

func (s *ingesterTransferChunkStreamMock) CloseAndRecv() (*client.TransferChunksResponse, error) {
	close(s.reqs)
	select {
	case resp := <-s.resp:
		return resp, nil
	case err := <-s.err:
		return nil, err
	}
}

func (s *ingesterTransferChunkStreamMock) SendAndClose(resp *client.TransferChunksResponse) error {
	s.resp <- resp
	return nil
}

func (s *ingesterTransferChunkStreamMock) ErrorAndClose(err error) {
	s.err <- err
}

func (s *ingesterTransferChunkStreamMock) Recv() (*client.TimeSeriesChunk, error) {
	req, ok := <-s.reqs
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

func (s *ingesterTransferChunkStreamMock) Context() context.Context {
	return s.ctx
}

func (*ingesterTransferChunkStreamMock) SendMsg(m interface{}) error {
	return nil
}

func (*ingesterTransferChunkStreamMock) RecvMsg(m interface{}) error {
	return nil
}

type ingesterClientAdapater struct {
	client.IngesterClient
	ingester client.IngesterServer
}

func (i ingesterClientAdapater) TransferChunks(ctx context.Context, _ ...grpc.CallOption) (client.Ingester_TransferChunksClient, error) {
	stream := &ingesterTransferChunkStreamMock{
		ctx:  ctx,
		reqs: make(chan *client.TimeSeriesChunk),
		resp: make(chan *client.TransferChunksResponse),
		err:  make(chan error),
	}
	go func() {
		err := i.ingester.TransferChunks(stream)
		if err != nil {
			stream.ErrorAndClose(err)
		}
	}()
	return stream, nil
}

func (i ingesterClientAdapater) Close() error {
	return nil
}

// TestIngesterFlush tries to test that the ingester flushes chunks before
// removing itself from the ring.
func TestIngesterFlush(t *testing.T) {
	// Start the ingester, and get it into ACTIVE state.
	store, ing := newTestStore(t, defaultIngesterTestConfig())

	poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return ing.lifecycler.GetState()
	})

	// Now write a sample to this ingester
	var (
		ts  = model.TimeFromUnix(123)
		val = model.SampleValue(456)
		m   = model.Metric{
			model.MetricNameLabel: "foo",
		}
	)
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, client.ToWriteRequest([]model.Sample{
		{
			Metric:    m,
			Timestamp: ts,
			Value:     val,
		},
	}))
	require.NoError(t, err)

	// We add a 100ms sleep into the flush loop, such that we can reliably detect
	// if the ingester is removing its token from Consul before flushing chunks.
	ing.preFlushUserSeries = func() {
		time.Sleep(100 * time.Millisecond)
	}

	// Now stop the ingester.  Don't call shutdown, as it waits for all goroutines
	// to exit.  We just want to check that by the time the token is removed from
	// the ring, the data is in the chunk store.
	ing.lifecycler.Shutdown()
	poll(t, 200*time.Millisecond, 0, func() interface{} {
		r, err := ing.lifecycler.KVStore.Get(context.Background(), ring.ConsulKey)
		if err != nil {
			return -1
		}
		return len(r.(*ring.Desc).Ingesters)
	})

	// And check the store has the chunk
	res, err := chunk.ChunksToMatrix(context.Background(), store.chunks[userID], model.Time(0), model.Time(math.MaxInt64))
	require.NoError(t, err)
	assert.Equal(t, model.Matrix{
		&model.SampleStream{
			Metric: model.Metric{
				model.MetricNameLabel: "foo",
			},
			Values: []model.SamplePair{
				{Timestamp: model.TimeFromUnix(123), Value: model.SampleValue(456)},
			},
		},
	}, res)
}
