package ingester

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	rnd "math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/shipper"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type testUserTSDB struct {
	userID      string
	shipPercent int
	numBlocks   int
	meta        *shipper.Meta
	unshipped   []string
}

func createTSDB(t *testing.T, dir string, users []*testUserTSDB) {
	for _, user := range users {

		os.MkdirAll(filepath.Join(dir, user.userID), 0777)

		for i := 0; i < user.numBlocks; i++ {
			u, err := ulid.New(uint64(time.Now().Unix()*1000), rand.Reader)
			require.NoError(t, err)

			userdir := filepath.Join(dir, user.userID)
			blockDir := filepath.Join(userdir, u.String())
			require.NoError(t, os.MkdirAll(filepath.Join(blockDir, "chunks"), 0777))

			createAndWrite := func(t *testing.T, path string) {
				f, err := os.Create(path)
				require.NoError(t, err)
				defer f.Close()
				_, err = f.Write([]byte("a man a plan a canal panama"))
				require.NoError(t, err)
			}

			for i := 0; i < 2; i++ {
				createAndWrite(t, filepath.Join(blockDir, "chunks", fmt.Sprintf("00000%v", i)))
			}

			meta := []string{"index", "meta.json", "tombstones"}
			for _, name := range meta {
				createAndWrite(t, filepath.Join(blockDir, name))
			}

			require.NoError(t, os.MkdirAll(filepath.Join(userdir, "wal", "checkpoint.000419"), 0777))
			createAndWrite(t, filepath.Join(userdir, "wal", "000001"))
			createAndWrite(t, filepath.Join(userdir, "wal", "checkpoint.000419", "000000"))

			// Record if this block is to be "shipped"
			if rnd.Intn(100) < user.shipPercent {
				user.meta.Uploaded = append(user.meta.Uploaded, u)
			} else {
				user.unshipped = append(user.unshipped, u.String())
			}
		}

		require.NoError(t, shipper.WriteMetaFile(nil, filepath.Join(dir, user.userID), user.meta))
	}
}

func TestUnshippedBlocks(t *testing.T) {
	dir, err := ioutil.TempDir("", "tsdb")
	require.NoError(t, err)

	// Validate empty dir
	blks, err := unshippedBlocks(dir)
	require.NoError(t, err)
	require.Empty(t, blks)

	/*
		Create three user dirs
		One of them has some blocks shipped,
		One of them has all blocks shipped,
		One of them has no blocks shipped,
	*/
	users := []*testUserTSDB{
		{
			userID:      "0",
			shipPercent: 70,
			numBlocks:   10,
			meta: &shipper.Meta{
				Version: shipper.MetaVersion1,
			},
			unshipped: []string{},
		},
		{
			userID:      "1",
			shipPercent: 100,
			numBlocks:   10,
			meta: &shipper.Meta{
				Version: shipper.MetaVersion1,
			},
			unshipped: []string{},
		},
		{
			userID:      "2",
			shipPercent: 0,
			numBlocks:   10,
			meta: &shipper.Meta{
				Version: shipper.MetaVersion1,
			},
			unshipped: []string{},
		},
	}

	createTSDB(t, dir, users)

	blks, err = unshippedBlocks(dir)
	require.NoError(t, err)
	for _, u := range users {
		_, ok := blks[u.userID]
		require.True(t, ok)
	}

	// Validate the unshipped blocks against the returned list
	for _, user := range users {
		require.ElementsMatch(t, user.unshipped, blks[user.userID])
	}
}

type MockTransferTSDBClient struct {
	Dir string

	grpc.ClientStream
}

func (m *MockTransferTSDBClient) Send(f *client.TimeSeriesFile) error {
	dir, _ := filepath.Split(f.Filename)
	if err := os.MkdirAll(filepath.Join(m.Dir, dir), 0777); err != nil {
		return err
	}
	if _, err := os.Create(filepath.Join(m.Dir, f.Filename)); err != nil {
		return err
	}
	return nil
}

func (m *MockTransferTSDBClient) CloseAndRecv() (*client.TransferTSDBResponse, error) {
	return &client.TransferTSDBResponse{}, nil
}

func TestTransferUser(t *testing.T) {
	dir, err := ioutil.TempDir("", "tsdb")
	require.NoError(t, err)

	createTSDB(t, dir, []*testUserTSDB{
		{
			userID:      "0",
			shipPercent: 0,
			numBlocks:   3,
			meta: &shipper.Meta{
				Version: shipper.MetaVersion1,
			},
		},
	})

	blks, err := unshippedBlocks(dir)
	require.NoError(t, err)

	xfer, err := ioutil.TempDir("", "xfer")
	require.NoError(t, err)
	m := &MockTransferTSDBClient{
		Dir: xfer,
	}
	transferUser(context.Background(), m, dir, "test", "0", blks["0"])

	var original []string
	var xferfiles []string
	filepath.Walk(xfer, func(path string, info os.FileInfo, err error) error {
		p, _ := filepath.Rel(xfer, path)
		xferfiles = append(xferfiles, p)
		return nil
	})

	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.Name() == "thanos.shipper.json" {
			return nil
		}
		p, _ := filepath.Rel(dir, path)
		original = append(original, p)
		return nil
	})

	require.Equal(t, original, xferfiles)
}

type testTimeSeriesClient struct {
	ctx context.Context
	ch  chan *client.TimeSeriesChunk
	grpc.ClientStream
}

func (s *testTimeSeriesClient) Context() context.Context { return s.ctx }

func (s *testTimeSeriesClient) Recv() (*client.TimeSeriesChunk, error) {
	ret, ok := <-s.ch
	if !ok {
		return nil, io.EOF
	}
	return ret, nil
}

func (s *testTimeSeriesClient) Send(resp *client.TimeSeriesChunk) error {
	s.ch <- resp
	return nil
}

type testTimeSeriesServer struct {
	ctx context.Context
	ch  chan *client.TimeSeriesChunk
	grpc.ServerStream
}

func (s *testTimeSeriesServer) Context() context.Context { return s.ctx }

func (s *testTimeSeriesServer) Recv() (*client.TimeSeriesChunk, error) {
	ret, ok := <-s.ch
	if !ok {
		return nil, io.EOF
	}
	return ret, nil
}

func (s *testTimeSeriesServer) Send(resp *client.TimeSeriesChunk) error {
	s.ch <- resp
	return nil
}

type testTransferChunksClient struct {
	testTimeSeriesClient

	resp chan *client.TransferChunksResponse
}

func (s *testTransferChunksClient) CloseAndRecv() (*client.TransferChunksResponse, error) {
	close(s.ch)
	resp := <-s.resp
	close(s.resp)
	return resp, nil
}

type testTransferChunksServer struct {
	testTimeSeriesServer

	resp chan *client.TransferChunksResponse
}

func (s *testTransferChunksServer) SendAndClose(resp *client.TransferChunksResponse) error {
	s.resp <- resp
	return nil
}

func makeDummyPushData() *client.WriteRequest {
	return &client.WriteRequest{
		Source: client.API,
		Timeseries: []client.PreallocTimeseries{
			{
				TimeSeries: &client.TimeSeries{
					Labels: []client.LabelAdapter{
						{Name: "__name__", Value: "metric"},
						{Name: "foo", Value: "bar"},
						{Name: "bar", Value: "baz1"},
					},
					Samples: []client.Sample{
						{Value: 10, TimestampMs: time.Unix(0, 0).Unix() * 1000},
						{Value: 20, TimestampMs: time.Unix(1, 0).Unix() * 1000},
					},
					Token: 1234,
				},
			},
			{
				TimeSeries: &client.TimeSeries{
					Labels: []client.LabelAdapter{
						{Name: "__name__", Value: "metric"},
						{Name: "foo", Value: "bar"},
						{Name: "bar", Value: "baz2"},
					},
					Samples: []client.Sample{
						{Value: 30, TimestampMs: time.Unix(2, 0).Unix() * 1000},
						{Value: 40, TimestampMs: time.Unix(3, 0).Unix() * 1000},
					},
					Token: 1234,
				},
			},
		},
	}
}

func TestSendChunkRange(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "test")
	f := newTestIngesterFactory(t)

	ing := f.getIngester(t)
	defer ing.Shutdown()
	_, err := ing.Push(ctx, makeDummyPushData())
	require.NoError(t, err)

	statesCp := ing.userStates.cp()
	require.Len(t, statesCp, 1)
	require.Equal(t, 2, statesCp["test"].fpToSeries.length())

	ing2 := f.getIngester(t)
	defer ing2.Shutdown()

	// First, try sending zero chunks
	err = ing.SendChunkRanges(ctx, []ring.TokenRange{{From: 0, To: 0}}, ing2.lifecycler.ID)
	require.NoError(t, err)

	statesCp = ing2.userStates.cp()
	require.Len(t, statesCp, 0)

	// Now, send all chunks
	err = ing.SendChunkRanges(ctx, []ring.TokenRange{{From: 0, To: math.MaxUint32}}, ing2.lifecycler.ID)
	require.NoError(t, err)

	statesCp = ing2.userStates.cp()
	require.Len(t, statesCp, 1)
	require.Equal(t, 2, statesCp["test"].fpToSeries.length())
}

func TestRequestChunkRange(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "test")
	f := newTestIngesterFactory(t)

	ing := f.getIngester(t)
	defer ing.Shutdown()

	_, err := ing.Push(ctx, makeDummyPushData())
	require.NoError(t, err)

	statesCp := ing.userStates.cp()
	require.Len(t, statesCp, 1)
	require.Equal(t, 2, statesCp["test"].fpToSeries.length())

	ing2 := f.getIngester(t)
	defer ing2.Shutdown()

	// First, try requesting zero chunks
	err = ing2.RequestChunkRanges(ctx, []ring.TokenRange{{From: 0, To: 0}}, ing.lifecycler.ID, false)
	require.NoError(t, err)

	statesCp = ing2.userStates.cp()
	require.Len(t, statesCp, 0)

	// Now, request all chunks
	err = ing2.RequestChunkRanges(ctx, []ring.TokenRange{{From: 0, To: math.MaxUint32}}, ing.lifecycler.ID, false)
	require.NoError(t, err)

	statesCp = ing2.userStates.cp()
	require.Len(t, statesCp, 1)
	require.Equal(t, 2, statesCp["test"].fpToSeries.length())

	// Should still be in the first ingester from the copy
	statesCp = ing.userStates.cp()
	require.Len(t, statesCp, 1)
	require.Equal(t, 2, statesCp["test"].fpToSeries.length())

	// Transfer again with a move and make sure they're gone
	// from the first ingester
	err = ing2.RequestChunkRanges(ctx, []ring.TokenRange{{From: 0, To: math.MaxUint32}}, ing.lifecycler.ID, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(ing.MemoryStreamTokens()))
}

func TestBlockRange(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "test")
	f := newTestIngesterFactory(t)

	ing := f.getIngester(t)
	defer ing.Shutdown()

	// Block nothing
	ing.BlockRanges([]ring.TokenRange{{From: 0, To: 0}})

	_, err := ing.Push(ctx, makeDummyPushData())
	require.NoError(t, err)

	_, err = ing.UnblockRanges(ctx, &client.UnblockRangesRequest{
		Ranges: []ring.TokenRange{{From: 0, To: 0}},
	})
	require.NoError(t, err)

	ing.BlockRanges([]ring.TokenRange{{From: 0, To: math.MaxUint32}})
	require.NoError(t, err)

	_, err = ing.Push(ctx, makeDummyPushData())
	require.Error(t, err)
}

type testIngesterFactory struct {
	ReplicationFactor int
	GenerateTokens    ring.TokenGeneratorFunc
	NumTokens         int

	t     require.TestingT
	store kv.Client
	n     int

	ingestersMtx sync.Mutex
	ingesters    map[string]*Ingester
}

func newTestIngesterFactory(t require.TestingT) *testIngesterFactory {
	kvClient, err := kv.NewClient(kv.Config{Store: "inmemory"}, codec.Proto{Factory: ring.ProtoDescFactory})
	require.NoError(t, err)

	return &testIngesterFactory{
		ReplicationFactor: 1,
		NumTokens:         1,

		t:         t,
		store:     kvClient,
		ingesters: make(map[string]*Ingester),
	}
}

func (f *testIngesterFactory) getClient(addr string, cfg client.Config) (client.HealthAndIngesterClient, error) {
	f.ingestersMtx.Lock()
	defer f.ingestersMtx.Unlock()

	ingester, ok := f.ingesters[addr]
	if !ok {
		return nil, fmt.Errorf("no ingester %s", addr)
	}

	return struct {
		client.IngesterClient
		grpc_health_v1.HealthClient
		io.Closer
	}{
		IngesterClient: &testIngesterClient{i: ingester},
		Closer:         ioutil.NopCloser(nil),
	}, nil
}

func (f *testIngesterFactory) getIngester(t require.TestingT) *Ingester {
	currentIngesters := f.n
	f.n++

	cfg := defaultIngesterTestConfig()
	cfg.MaxTransferRetries = 1
	cfg.LifecyclerConfig.NumTokens = f.NumTokens
	cfg.LifecyclerConfig.ID = fmt.Sprintf("localhost-%d", f.n)
	cfg.LifecyclerConfig.JoinIncrementalTransfer = true
	cfg.LifecyclerConfig.LeaveIncrementalTransfer = true
	cfg.LifecyclerConfig.Addr = cfg.LifecyclerConfig.ID
	cfg.LifecyclerConfig.RingConfig.KVStore.Mock = f.store
	cfg.LifecyclerConfig.RingConfig.ReplicationFactor = f.ReplicationFactor
	cfg.LifecyclerConfig.TransferFinishDelay = time.Duration(0)

	// Assign incrementally valued tokens to each ingester.
	cfg.LifecyclerConfig.GenerateTokens =
		func(numTokens int, taken []uint32) []uint32 {
			value := uint32(currentIngesters + 1)
			var tokens []uint32
			for i := 0; i < numTokens; i++ {
				tokens = append(tokens, value)
				value++
			}
			return tokens
		}

	if f.GenerateTokens != nil {
		cfg.LifecyclerConfig.GenerateTokens = f.GenerateTokens
	}

	cfg.ingesterClientFactory = f.getClient

	_, ing := newTestStore(f.t, cfg, defaultClientTestConfig(), defaultLimitsTestConfig())

	f.ingestersMtx.Lock()
	defer f.ingestersMtx.Unlock()

	f.ingesters[fmt.Sprintf("%s", cfg.LifecyclerConfig.ID)] = ing
	f.ingesters[fmt.Sprintf("%s:0", cfg.LifecyclerConfig.ID)] = ing

	// NB there's some kind of race condition with the in-memory KV client when
	// we don't give the ingester a little bit of time to initialize. a 100ms
	// wait time seems effective.
	time.Sleep(time.Millisecond * 100)
	return ing
}

type testIngesterClient struct {
	i *Ingester

	client.IngesterClient
}

func (c *testIngesterClient) Push(ctx context.Context, in *client.WriteRequest, opts ...grpc.CallOption) (*client.WriteResponse, error) {
	return c.i.Push(ctx, in)
}

func (c *testIngesterClient) Query(ctx context.Context, in *client.QueryRequest, opts ...grpc.CallOption) (*client.QueryResponse, error) {
	return c.i.Query(ctx, in)
}

type testIngesterQueryStreamClient struct {
	ctx context.Context
	ch  chan *client.QueryStreamResponse

	grpc.ClientStream
}

func (c *testIngesterQueryStreamClient) Context() context.Context {
	return c.ctx
}

func (c *testIngesterQueryStreamClient) Recv() (*client.QueryStreamResponse, error) {
	resp, ok := <-c.ch
	if !ok {
		return nil, io.EOF
	}
	return resp, nil
}

type testIngesterQueryStreamServer struct {
	ctx context.Context
	ch  chan *client.QueryStreamResponse

	grpc.ServerStream
}

func (s *testIngesterQueryStreamServer) Context() context.Context {
	return s.ctx
}

func (s *testIngesterQueryStreamServer) Send(resp *client.QueryStreamResponse) error {
	s.ch <- resp
	return nil
}

func (c *testIngesterClient) QueryStream(ctx context.Context, in *client.QueryRequest, opts ...grpc.CallOption) (client.Ingester_QueryStreamClient, error) {
	ch := make(chan *client.QueryStreamResponse)

	go func() {
		srv := testIngesterQueryStreamServer{ctx: ctx, ch: ch}
		c.i.QueryStream(in, &srv)
	}()

	cli := testIngesterQueryStreamClient{ch: ch}
	return &cli, nil
}

func (c *testIngesterClient) LabelValues(ctx context.Context, in *client.LabelValuesRequest, opts ...grpc.CallOption) (*client.LabelValuesResponse, error) {
	return c.i.LabelValues(ctx, in)
}

func (c *testIngesterClient) LabelNames(ctx context.Context, in *client.LabelNamesRequest, opts ...grpc.CallOption) (*client.LabelNamesResponse, error) {
	return c.i.LabelNames(ctx, in)
}

func (c *testIngesterClient) UserStats(ctx context.Context, in *client.UserStatsRequest, opts ...grpc.CallOption) (*client.UserStatsResponse, error) {
	return c.i.UserStats(ctx, in)
}

func (c *testIngesterClient) AllUserStats(ctx context.Context, in *client.UserStatsRequest, opts ...grpc.CallOption) (*client.UsersStatsResponse, error) {
	return c.i.AllUserStats(ctx, in)
}

func (c *testIngesterClient) MetricsForLabelMatchers(ctx context.Context, in *client.MetricsForLabelMatchersRequest, opts ...grpc.CallOption) (*client.MetricsForLabelMatchersResponse, error) {
	return c.i.MetricsForLabelMatchers(ctx, in)
}

func (c *testIngesterClient) TransferChunks(ctx context.Context, opts ...grpc.CallOption) (client.Ingester_TransferChunksClient, error) {
	ch := make(chan *client.TimeSeriesChunk)
	resp := make(chan *client.TransferChunksResponse)

	srv := testTransferChunksServer{
		testTimeSeriesServer: testTimeSeriesServer{ctx: ctx, ch: ch},
		resp:                 resp,
	}

	cli := testTransferChunksClient{
		testTimeSeriesClient: testTimeSeriesClient{ctx: ctx, ch: ch},
		resp:                 resp,
	}

	go func() {
		c.i.TransferChunks(&srv)
	}()

	return &cli, nil
}

type testSendChunksClient struct {
	testTimeSeriesClient

	resp chan *client.TransferChunksResponse
}

func (s *testSendChunksClient) CloseAndRecv() (*client.TransferChunksResponse, error) {
	close(s.ch)
	resp := <-s.resp
	close(s.resp)
	return resp, nil
}

type testSendChunksServer struct {
	testTimeSeriesServer

	resp chan *client.TransferChunksResponse
}

func (s *testSendChunksServer) SendAndClose(resp *client.TransferChunksResponse) error {
	s.resp <- resp
	return nil
}

func (c *testIngesterClient) TransferChunksSubset(ctx context.Context, opts ...grpc.CallOption) (client.Ingester_TransferChunksSubsetClient, error) {
	ch := make(chan *client.TimeSeriesChunk)
	resp := make(chan *client.TransferChunksResponse)

	srv := testSendChunksServer{
		testTimeSeriesServer: testTimeSeriesServer{ctx: ctx, ch: ch},
		resp:                 resp,
	}

	cli := testSendChunksClient{
		testTimeSeriesClient: testTimeSeriesClient{ctx: ctx, ch: ch},
		resp:                 resp,
	}

	go func() {
		c.i.TransferChunksSubset(&srv)
	}()

	return &cli, nil
}

func (c *testIngesterClient) GetChunksSubset(ctx context.Context, in *client.GetChunksRequest, opts ...grpc.CallOption) (client.Ingester_GetChunksSubsetClient, error) {
	ch := make(chan *client.TimeSeriesChunk)

	srv := testTimeSeriesServer{ctx: ctx, ch: ch}
	cli := testTimeSeriesClient{ctx: ctx, ch: ch}

	go func() {
		c.i.GetChunksSubset(in, &srv)
		close(ch)
	}()

	return &cli, nil
}
