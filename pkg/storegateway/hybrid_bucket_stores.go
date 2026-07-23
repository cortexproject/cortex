package storegateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid/v2"
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

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	cortex_util "github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/multierror"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type HybridBucketStores struct {
	logger log.Logger
	cfg    tsdb.BlocksStorageConfig
	limits *validation.Overrides
	bucket objstore.Bucket

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
			Help: "Total number of operations by which sub-store(s) served them.",
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

	// The TSDB store only syncs blocks not yet converted to Parquet (when the bucket index
	// is enabled, IgnoreParquetBlocksFilter excludes converted blocks).
	tsdbStore, err := newThanosBucketStores(cfg, shardingStrategy, bucketClient, cachingBucket, matcherCache, cfg.BucketStore.BucketIndex.Enabled, limits, logLevel, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create TSDB store for hybrid bucket stores")
	}

	parquetStore, err := newParquetBucketStores(cfg, bucketClient, cachingBucket, matcherCache, limits, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create parquet store for hybrid bucket stores")
	}

	return &HybridBucketStores{
		logger:           logger,
		cfg:              cfg,
		limits:           limits,
		bucket:           cachingBucket,
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
func (h *HybridBucketStores) splitRequestedBlocks(ctx context.Context, userID string, blockMatchers []storepb.LabelMatcher, method string) (parquetIDs, tsdbIDs []string, err error) {
	if len(blockMatchers) != 1 || blockMatchers[0].Type != storepb.LabelMatcher_RE || blockMatchers[0].Name != block.BlockIDLabel {
		return nil, nil, status.Error(codes.InvalidArgument, "only one block matcher is supported")
	}

	blockIDs := strings.Split(blockMatchers[0].Value, "|")
	filtered := blockIDs[:0]
	for _, id := range blockIDs {
		if id != "" {
			filtered = append(filtered, id)
		}
	}
	blockIDs = filtered

	isParquet, err := h.parquetBlocks(ctx, userID, blockIDs)
	if err != nil {
		return nil, nil, err
	}

	for _, id := range blockIDs {
		if isParquet[id] {
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

// parquetBlocks returns, for each requested block ID, whether it is served by the Parquet store.
func (h *HybridBucketStores) parquetBlocks(ctx context.Context, userID string, blockIDs []string) (map[string]bool, error) {
	if dropped, ok := h.tsdb.droppedParquetBlocks(userID); ok {
		result := make(map[string]bool, len(blockIDs))
		for _, id := range blockIDs {
			_, isParquet := dropped[id]
			result[id] = isParquet
		}
		return result, nil
	}

	return h.parquetBlocksFromConverterMarks(ctx, userID, blockIDs)
}

// parquetBlocksFromConverterMarks classifies each block by reading its converter mark directly.
// A missing mark (Version == 0) means the block has not been converted to Parquet yet.
func (h *HybridBucketStores) parquetBlocksFromConverterMarks(ctx context.Context, userID string, blockIDs []string) (map[string]bool, error) {
	result := make(map[string]bool, len(blockIDs))
	userBkt := bucket.NewUserBucketClient(userID, h.bucket, h.limits)
	for _, id := range blockIDs {
		uid, err := ulid.Parse(id)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse block ID %s", id)
		}
		marker, err := cortex_parquet.ReadConverterMark(ctx, uid, userBkt, h.logger)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read converter mark for block %s", id)
		}
		result[id] = marker.Version > 0
	}

	return result, nil
}

// blockIDsMatcher builds the single regex block matcher understood by the stores.
func blockIDsMatcher(blockIDs []string) storepb.LabelMatcher {
	return storepb.LabelMatcher{
		Type:  storepb.LabelMatcher_RE,
		Name:  block.BlockIDLabel,
		Value: strings.Join(blockIDs, "|"),
	}
}

// seriesWithTSDBStore serves a Series request by splitting the requested blocks between the
// Parquet store and the TSDB store, then merging the (sorted) results.
func (h *HybridBucketStores) seriesWithTSDBStore(ctx context.Context, userID string, req *storepb.SeriesRequest, store *parquetBucketStore, srv storepb.Store_SeriesServer) error {
	var blockMatchers []storepb.LabelMatcher
	if req.Hints != nil {
		reqHints := &hintspb.SeriesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal series request hints").Error())
		}
		blockMatchers = reqHints.BlockMatchers
	}

	parquetIDs, tsdbIDs, err := h.splitRequestedBlocks(ctx, userID, blockMatchers, "Series")
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
	parquetSet := newChannelSeriesServer(gCtx, batchSize)
	tsdbSet := newChannelSeriesServer(gCtx, batchSize)
	g.Go(func() error {
		r, err := rewriteSeriesReqBlocks(req, parquetIDs)
		if err == nil {
			err = store.Series(r, parquetSet)
		}
		parquetSet.Close(err)
		return err
	})
	g.Go(func() error {
		r, err := rewriteSeriesReqBlocks(req, tsdbIDs)
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
	waitErr := g.Wait()

	producerErr := merged.Err()
	if producerErr == nil {
		producerErr = waitErr
	}

	switch {
	case sendErr != nil && producerErr != nil:
		return multierror.New(producerErr, sendErr).Err()
	case producerErr != nil:
		return producerErr
	case sendErr != nil:
		return sendErr
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

func rewriteSeriesReqBlocks(req *storepb.SeriesRequest, blockIDs []string) (*storepb.SeriesRequest, error) {
	reqHints := &hintspb.SeriesRequestHints{}
	if req.Hints != nil {
		// Best effort: ignore unmarshal error, we always overwrite the block matchers below.
		_ = types.UnmarshalAny(req.Hints, reqHints)
	}
	reqHints.BlockMatchers = []storepb.LabelMatcher{blockIDsMatcher(blockIDs)}

	anyHints, err := types.MarshalAny(reqHints)
	if err != nil {
		return nil, status.Error(codes.Internal, errors.Wrap(err, "marshal rewritten series request hints").Error())
	}
	clone := *req
	clone.Hints = anyHints
	return &clone, nil
}

// labelNamesWithTSDBStore serves a LabelNames request across the Parquet store and the TSDB store.
func (h *HybridBucketStores) labelNamesWithTSDBStore(ctx context.Context, userID string, req *storepb.LabelNamesRequest, store *parquetBucketStore) (*storepb.LabelNamesResponse, error) {
	var blockMatchers []storepb.LabelMatcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelNamesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label names request hints").Error())
		}
		blockMatchers = reqHints.BlockMatchers
	}

	parquetIDs, tsdbIDs, err := h.splitRequestedBlocks(ctx, userID, blockMatchers, "LabelNames")
	if err != nil {
		return nil, err
	}

	switch {
	case len(tsdbIDs) == 0:
		return store.LabelNames(ctx, req)
	case len(parquetIDs) == 0:
		return h.tsdb.LabelNames(ctx, req)
	}

	var (
		parquetResp, tsdbResp *storepb.LabelNamesResponse
	)
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		r, err := rewriteLabelNamesReqBlocks(req, parquetIDs)
		if err != nil {
			return err
		}
		parquetResp, err = store.LabelNames(gCtx, r)
		return err
	})
	g.Go(func() error {
		r, err := rewriteLabelNamesReqBlocks(req, tsdbIDs)
		if err != nil {
			return err
		}
		tsdbResp, err = h.tsdb.LabelNames(gCtx, r)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	names := parquet_util.MergeUnsortedSlices(int(req.Limit), parquetResp.Names, tsdbResp.Names)
	anyHints, err := mergeLabelNamesHints(parquetResp, tsdbResp)
	if err != nil {
		return nil, err
	}
	return &storepb.LabelNamesResponse{
		Names:    names,
		Warnings: append(parquetResp.Warnings, tsdbResp.Warnings...),
		Hints:    anyHints,
	}, nil
}

func rewriteLabelNamesReqBlocks(req *storepb.LabelNamesRequest, blockIDs []string) (*storepb.LabelNamesRequest, error) {
	reqHints := &hintspb.LabelNamesRequestHints{}
	if req.Hints != nil {
		_ = types.UnmarshalAny(req.Hints, reqHints)
	}
	reqHints.BlockMatchers = []storepb.LabelMatcher{blockIDsMatcher(blockIDs)}

	anyHints, err := types.MarshalAny(reqHints)
	if err != nil {
		return nil, status.Error(codes.Internal, errors.Wrap(err, "marshal rewritten label names request hints").Error())
	}
	clone := *req
	clone.Hints = anyHints
	return &clone, nil
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
	var blockMatchers []storepb.LabelMatcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelValuesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label values request hints").Error())
		}
		blockMatchers = reqHints.BlockMatchers
	}

	parquetIDs, tsdbIDs, err := h.splitRequestedBlocks(ctx, userID, blockMatchers, "LabelValues")
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
		r, err := rewriteLabelValuesReqBlocks(req, parquetIDs)
		if err != nil {
			return err
		}
		parquetResp, err = store.LabelValues(gCtx, r)
		return err
	})
	g.Go(func() error {
		r, err := rewriteLabelValuesReqBlocks(req, tsdbIDs)
		if err != nil {
			return err
		}
		tsdbResp, err = h.tsdb.LabelValues(gCtx, r)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	values := parquet_util.MergeUnsortedSlices(int(req.Limit), parquetResp.Values, tsdbResp.Values)
	anyHints, err := mergeLabelValuesHints(parquetResp, tsdbResp)
	if err != nil {
		return nil, err
	}
	return &storepb.LabelValuesResponse{
		Values:   values,
		Warnings: append(parquetResp.Warnings, tsdbResp.Warnings...),
		Hints:    anyHints,
	}, nil
}

func rewriteLabelValuesReqBlocks(req *storepb.LabelValuesRequest, blockIDs []string) (*storepb.LabelValuesRequest, error) {
	reqHints := &hintspb.LabelValuesRequestHints{}
	if req.Hints != nil {
		_ = types.UnmarshalAny(req.Hints, reqHints)
	}
	reqHints.BlockMatchers = []storepb.LabelMatcher{blockIDsMatcher(blockIDs)}

	anyHints, err := types.MarshalAny(reqHints)
	if err != nil {
		return nil, status.Error(codes.Internal, errors.Wrap(err, "marshal rewritten label values request hints").Error())
	}
	clone := *req
	clone.Hints = anyHints
	return &clone, nil
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
