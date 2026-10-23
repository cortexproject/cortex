package storegateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	parquet_util "github.com/prometheus-community/parquet-common/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/user"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	cortex_util "github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/multierror"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type HybridBucketStores struct {
	cfg tsdb.BlocksStorageConfig

	inflightRequests *cortex_util.InflightRequestTracker

	parquet *ParquetBucketStores
	tsdb    *ThanosBucketStores

	metrics *hybridBucketStoresMetrics
}

type hybridBucketStoresMetrics struct {
	blocksRoutedTotal *prometheus.CounterVec
	operationsTotal   *prometheus.CounterVec
}

func newHybridBucketStoresMetrics(reg prometheus.Registerer) *hybridBucketStoresMetrics {
	return &hybridBucketStoresMetrics{
		blocksRoutedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_hybrid_bucket_stores_blocks_routed_total",
			Help: "Total number of requested blocks routed to each sub-store.",
		}, []string{"store"}),
		operationsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_hybrid_bucket_stores_operations_total",
			Help: "Total number of operations served, labeled by method and which sub-store(s) served them.",
		}, []string{"store", "method"}),
	}
}

func newHybridBucketStores(cfg tsdb.BlocksStorageConfig, shardingStrategy ShardingStrategy, bucketClient objstore.InstrumentedBucket, limits *validation.Overrides, logLevel logging.Level, logger log.Logger, reg prometheus.Registerer) (*HybridBucketStores, error) {
	cachingBucket, err := createCachingBucketClientForParquet(cfg, bucketClient, "parquet-storegateway", logger, reg)
	if err != nil {
		return nil, err
	}
	matcherCache, err := newMatchersCache(cfg, logger, reg)
	if err != nil {
		return nil, err
	}

	// The TSDB store only syncs blocks not yet converted to Parquet.
	tsdbStore, err := newThanosBucketStores(cfg, shardingStrategy, bucketClient, cachingBucket, matcherCache, true, limits, logLevel, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create TSDB store for hybrid bucket stores")
	}

	parquetStore, err := newParquetBucketStores(cfg, bucketClient, cachingBucket, matcherCache, limits, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create parquet store for hybrid bucket stores")
	}

	return &HybridBucketStores{
		cfg:              cfg,
		inflightRequests: cortex_util.NewInflightRequestTracker(),
		parquet:          parquetStore,
		tsdb:             tsdbStore,
		metrics:          newHybridBucketStoresMetrics(reg),
	}, nil
}

// Series implements BucketStores.
func (h *HybridBucketStores) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	spanLog, spanCtx := spanlogger.New(srv.Context(), "HybridBucketStores.Series")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return fmt.Errorf("no userID")
	}
	spanCtx = user.InjectOrgID(spanCtx, userID)

	if err := h.checkStoreError(userID); err != nil {
		return err
	}

	store, err := h.parquet.getOrCreateStore(userID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	if maxInflightRequests := h.cfg.BucketStore.MaxInflightRequests; maxInflightRequests > 0 {
		if h.inflightRequests.Count() >= maxInflightRequests {
			return ErrTooManyInflightRequests
		}
		h.inflightRequests.Inc()
		defer h.inflightRequests.Dec()
	}

	wrappedSrv := spanSeriesServer{
		Store_SeriesServer: srv,
		ctx:                spanCtx,
	}
	return h.seriesWithTSDBStore(spanCtx, userID, req, store, wrappedSrv)
}

// LabelNames implements BucketStores.
func (h *HybridBucketStores) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	spanLog, spanCtx := spanlogger.New(ctx, "HybridBucketStores.LabelNames")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}
	spanCtx = user.InjectOrgID(spanCtx, userID)

	if err := h.checkStoreError(userID); err != nil {
		return nil, err
	}

	store, err := h.parquet.getOrCreateStore(userID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return h.labelNamesWithTSDBStore(spanCtx, userID, req, store)
}

// LabelValues implements BucketStores.
func (h *HybridBucketStores) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	spanLog, spanCtx := spanlogger.New(ctx, "HybridBucketStores.LabelValues")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}
	spanCtx = user.InjectOrgID(spanCtx, userID)

	if err := h.checkStoreError(userID); err != nil {
		return nil, err
	}

	store, err := h.parquet.getOrCreateStore(userID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return h.labelValuesWithTSDBStore(spanCtx, userID, req, store)
}

func (h *HybridBucketStores) checkStoreError(userID string) error {
	if err := h.parquet.getStoreError(userID); err != nil {
		return err
	}
	return h.tsdb.getStoreError(userID)
}

// SyncBlocks implements BucketStores.
func (h *HybridBucketStores) SyncBlocks(ctx context.Context) error {
	return h.tsdb.SyncBlocks(ctx)
}

// InitialSync implements BucketStores.
func (h *HybridBucketStores) InitialSync(ctx context.Context) error {
	if err := h.parquet.InitialSync(ctx); err != nil {
		return err
	}
	return h.tsdb.InitialSync(ctx)
}

// Stop implements BucketStores
func (h *HybridBucketStores) Stop() error {
	return multierror.New(h.parquet.Stop(), h.tsdb.Stop()).Err()
}

// splitRequestedBlocks splits the block IDs encoded in the request block matchers into
// Parquet-converted and not-yet-converted (TSDB) groups.
func (h *HybridBucketStores) splitRequestedBlocks(userID string, blockMatchers []storepb.LabelMatcher, method string) (parquetIDs, tsdbIDs []string, err error) {
	if len(blockMatchers) != 1 || blockMatchers[0].Type != storepb.LabelMatcher_RE || blockMatchers[0].Name != block.BlockIDLabel {
		return nil, nil, status.Error(codes.InvalidArgument, "only one block matcher is supported")
	}

	// Nil until the tenant's first sync, so every block is routed to the TSDB store in that window,
	// which matches how a TSDB-only store-gateway behaves.
	parquetBlockIDs := h.tsdb.parquetBlockIDs(userID)
	for id := range strings.SplitSeq(blockMatchers[0].Value, "|") {
		if id == "" {
			continue
		}
		if _, ok := parquetBlockIDs[id]; ok {
			parquetIDs = append(parquetIDs, id)
		} else {
			tsdbIDs = append(tsdbIDs, id)
		}
	}

	h.metrics.blocksRoutedTotal.WithLabelValues("parquet").Add(float64(len(parquetIDs)))
	h.metrics.blocksRoutedTotal.WithLabelValues("tsdb").Add(float64(len(tsdbIDs)))
	switch {
	case len(parquetIDs) > 0 && len(tsdbIDs) > 0:
		h.metrics.operationsTotal.WithLabelValues("mixed", method).Inc()
	case len(parquetIDs) > 0:
		h.metrics.operationsTotal.WithLabelValues("parquet", method).Inc()
	case len(tsdbIDs) > 0:
		h.metrics.operationsTotal.WithLabelValues("tsdb", method).Inc()
	}

	return parquetIDs, tsdbIDs, nil
}

// blockIDsMatcher builds the single regex block matcher understood by the stores.
func blockIDsMatcher(blockIDs []string) []storepb.LabelMatcher {
	return []storepb.LabelMatcher{{
		Type:  storepb.LabelMatcher_RE,
		Name:  block.BlockIDLabel,
		Value: strings.Join(blockIDs, "|"),
	}}
}

// marshalSubRequestHints marshals the request hints of a sub-request sent to one of the stores.
func marshalSubRequestHints(hints proto.Message) (*types.Any, error) {
	anyHints, err := types.MarshalAny(hints)
	if err != nil {
		return nil, status.Error(codes.Internal, errors.Wrap(err, "marshal sub-request hints").Error())
	}
	return anyHints, nil
}

// seriesRequestForBlocks returns a copy of req whose parsed hints are restricted to blockIDs.
func seriesRequestForBlocks(req *storepb.SeriesRequest, hints *hintspb.SeriesRequestHints, blockIDs []string) (*storepb.SeriesRequest, error) {
	h := *hints
	h.BlockMatchers = blockIDsMatcher(blockIDs)
	anyHints, err := marshalSubRequestHints(&h)
	if err != nil {
		return nil, err
	}
	r := *req
	r.Hints = anyHints
	return &r, nil
}

// labelNamesRequestForBlocks returns a copy of req whose parsed hints are restricted to blockIDs.
func labelNamesRequestForBlocks(req *storepb.LabelNamesRequest, hints *hintspb.LabelNamesRequestHints, blockIDs []string) (*storepb.LabelNamesRequest, error) {
	h := *hints
	h.BlockMatchers = blockIDsMatcher(blockIDs)
	anyHints, err := marshalSubRequestHints(&h)
	if err != nil {
		return nil, err
	}
	r := *req
	r.Hints = anyHints
	return &r, nil
}

// labelValuesRequestForBlocks returns a copy of req whose parsed hints are restricted to blockIDs.
func labelValuesRequestForBlocks(req *storepb.LabelValuesRequest, hints *hintspb.LabelValuesRequestHints, blockIDs []string) (*storepb.LabelValuesRequest, error) {
	h := *hints
	h.BlockMatchers = blockIDsMatcher(blockIDs)
	anyHints, err := marshalSubRequestHints(&h)
	if err != nil {
		return nil, err
	}
	r := *req
	r.Hints = anyHints
	return &r, nil
}

// seriesWithTSDBStore serves a Series request by splitting the requested blocks between the
// Parquet store and the TSDB store, then merging the (sorted) results.
func (h *HybridBucketStores) seriesWithTSDBStore(ctx context.Context, userID string, req *storepb.SeriesRequest, store *parquetBucketStore, srv storepb.Store_SeriesServer) error {
	reqHints := &hintspb.SeriesRequestHints{}
	if req.Hints != nil {
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal series request hints").Error())
		}
	}

	parquetIDs, tsdbIDs, err := h.splitRequestedBlocks(userID, reqHints.BlockMatchers, "Series")
	if err != nil {
		return err
	}

	// If only one store has blocks to serve, delegate directly without buffering/merging.
	switch {
	case len(tsdbIDs) == 0:
		return store.Series(req, srv)
	case len(parquetIDs) == 0:
		return h.tsdb.Series(req, srv)
	}

	// Both stores return series sorted by labels. We run them concurrently, each pushing into a
	// channel-backed SeriesSet, and stream-merge their outputs so we never buffer either store's
	// full result set in memory.
	mergeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(mergeCtx)

	batchSize := int(req.ResponseBatchSize)
	// The Parquet store builds every series from memory it owns and never recycles, so its series
	// are retained as is. The TSDB store returns pooled chunk bytes once Series() returns.
	parquetSet := newChannelSeriesServer(gCtx, batchSize, false)
	tsdbSet := newChannelSeriesServer(gCtx, batchSize, true)
	g.Go(func() error {
		r, err := seriesRequestForBlocks(req, reqHints, parquetIDs)
		if err == nil {
			err = store.Series(r, parquetSet)
		}
		parquetSet.Close(err)
		return err
	})
	g.Go(func() error {
		r, err := seriesRequestForBlocks(req, reqHints, tsdbIDs)
		if err == nil {
			err = h.tsdb.Series(r, tsdbSet)
		}
		tsdbSet.Close(err)
		return err
	})

	outSrv := newFlushableServer(newBatchableServer(srv, batchSize))

	var sendErr error
	merged := storepb.MergeSeriesSets(parquetSet, tsdbSet)
	for merged.Next() {
		lset, chks := merged.At()
		if err := outSrv.Send(storepb.NewSeriesResponse(&storepb.Series{
			Labels: labelpb.ZLabelsFromPromLabels(lset),
			Chunks: chks,
		})); err != nil {
			sendErr = status.Error(codes.Unknown, errors.Wrap(err, "send merged series response").Error())
			break
		}
	}

	// Unblock and wait for the producers before inspecting their errors.
	cancel()
	// Use the errgroup error rather than merged.Err(): when one producer fails, the other one is
	// canceled, and merged.Err() may return that cancellation instead of the root cause.
	producerErr := g.Wait()

	// A send failure cancels the producers, so their errors are not the cause.
	if sendErr != nil {
		return sendErr
	}
	if producerErr != nil {
		return producerErr
	}

	// Forward accumulated warnings from both stores.
	warnings := parquetSet.warnings
	warnings.Merge(tsdbSet.warnings)
	for _, w := range warnings {
		if err := outSrv.Send(storepb.NewWarnSeriesResponse(w)); err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send merged series warning").Error())
		}
	}

	resHints := hintspb.SeriesResponseHints{
		QueriedBlocks: append(parquetSet.hints.QueriedBlocks, tsdbSet.hints.QueriedBlocks...),
	}
	if parquetSet.hints.QueryStats != nil || tsdbSet.hints.QueryStats != nil {
		stats := &hintspb.QueryStats{}
		if s := parquetSet.hints.QueryStats; s != nil {
			stats.Merge(s)
		}
		if s := tsdbSet.hints.QueryStats; s != nil {
			stats.Merge(s)
		}
		resHints.QueryStats = stats
	}
	anyHints, err := types.MarshalAny(&resHints)
	if err != nil {
		return status.Error(codes.Unknown, errors.Wrap(err, "marshal series response hints").Error())
	}
	if err := outSrv.Send(storepb.NewHintsSeriesResponse(anyHints)); err != nil {
		return status.Error(codes.Unknown, errors.Wrap(err, "send series response hints").Error())
	}

	return outSrv.Flush()
}

// labelNamesWithTSDBStore serves a LabelNames request across the Parquet store and the TSDB store.
func (h *HybridBucketStores) labelNamesWithTSDBStore(ctx context.Context, userID string, req *storepb.LabelNamesRequest, store *parquetBucketStore) (*storepb.LabelNamesResponse, error) {
	reqHints := &hintspb.LabelNamesRequestHints{}
	if req.Hints != nil {
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label names request hints").Error())
		}
	}

	parquetIDs, tsdbIDs, err := h.splitRequestedBlocks(userID, reqHints.BlockMatchers, "LabelNames")
	if err != nil {
		return nil, err
	}

	switch {
	case len(tsdbIDs) == 0:
		return store.LabelNames(ctx, req)
	case len(parquetIDs) == 0:
		return h.tsdb.LabelNames(ctx, req)
	}

	var parquetResp, tsdbResp *storepb.LabelNamesResponse
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		r, err := labelNamesRequestForBlocks(req, reqHints, parquetIDs)
		if err != nil {
			return err
		}
		parquetResp, err = store.LabelNames(gCtx, r)
		return err
	})
	g.Go(func() error {
		r, err := labelNamesRequestForBlocks(req, reqHints, tsdbIDs)
		if err != nil {
			return err
		}
		tsdbResp, err = h.tsdb.LabelNames(gCtx, r)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	anyHints, err := mergeLabelNamesHints(parquetResp, tsdbResp)
	if err != nil {
		return nil, err
	}
	return &storepb.LabelNamesResponse{
		Names:    parquet_util.MergeUnsortedSlices(int(req.Limit), parquetResp.Names, tsdbResp.Names),
		Warnings: append(parquetResp.Warnings, tsdbResp.Warnings...),
		Hints:    anyHints,
	}, nil
}

func mergeLabelNamesHints(a, b *storepb.LabelNamesResponse) (*types.Any, error) {
	merged := &hintspb.LabelNamesResponseHints{}
	for _, resp := range []*storepb.LabelNamesResponse{a, b} {
		if resp == nil || resp.Hints == nil {
			continue
		}
		hints := hintspb.LabelNamesResponseHints{}
		if err := types.UnmarshalAny(resp.Hints, &hints); err != nil {
			return nil, errors.Wrap(err, "unmarshal label names response hints")
		}
		merged.QueriedBlocks = append(merged.QueriedBlocks, hints.QueriedBlocks...)
	}
	return types.MarshalAny(merged)
}

// labelValuesWithTSDBStore serves a LabelValues request across the Parquet store and the TSDB store.
func (h *HybridBucketStores) labelValuesWithTSDBStore(ctx context.Context, userID string, req *storepb.LabelValuesRequest, store *parquetBucketStore) (*storepb.LabelValuesResponse, error) {
	reqHints := &hintspb.LabelValuesRequestHints{}
	if req.Hints != nil {
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label values request hints").Error())
		}
	}

	parquetIDs, tsdbIDs, err := h.splitRequestedBlocks(userID, reqHints.BlockMatchers, "LabelValues")
	if err != nil {
		return nil, err
	}

	switch {
	case len(tsdbIDs) == 0:
		return store.LabelValues(ctx, req)
	case len(parquetIDs) == 0:
		return h.tsdb.LabelValues(ctx, req)
	}

	var parquetResp, tsdbResp *storepb.LabelValuesResponse
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		r, err := labelValuesRequestForBlocks(req, reqHints, parquetIDs)
		if err != nil {
			return err
		}
		parquetResp, err = store.LabelValues(gCtx, r)
		return err
	})
	g.Go(func() error {
		r, err := labelValuesRequestForBlocks(req, reqHints, tsdbIDs)
		if err != nil {
			return err
		}
		tsdbResp, err = h.tsdb.LabelValues(gCtx, r)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	anyHints, err := mergeLabelValuesHints(parquetResp, tsdbResp)
	if err != nil {
		return nil, err
	}
	return &storepb.LabelValuesResponse{
		Values:   parquet_util.MergeUnsortedSlices(int(req.Limit), parquetResp.Values, tsdbResp.Values),
		Warnings: append(parquetResp.Warnings, tsdbResp.Warnings...),
		Hints:    anyHints,
	}, nil
}

func mergeLabelValuesHints(a, b *storepb.LabelValuesResponse) (*types.Any, error) {
	merged := &hintspb.LabelValuesResponseHints{}
	for _, resp := range []*storepb.LabelValuesResponse{a, b} {
		if resp == nil || resp.Hints == nil {
			continue
		}
		hints := hintspb.LabelValuesResponseHints{}
		if err := types.UnmarshalAny(resp.Hints, &hints); err != nil {
			return nil, errors.Wrap(err, "unmarshal label values response hints")
		}
		merged.QueriedBlocks = append(merged.QueriedBlocks, hints.QueriedBlocks...)
	}
	return types.MarshalAny(merged)
}
