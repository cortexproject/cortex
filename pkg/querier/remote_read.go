package querier

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage/remote"
	"io"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/storage"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

// Queries are a set of matchers with time ranges - should not get into megabytes
const maxRemoteReadQuerySize = 1024 * 1024

type remoteReadHandler struct {
	logger    log.Logger
	queryable storage.SampleAndChunkQueryable
	remoteReadMaxBytesInFrame int
}

// NewRemoteReadHandler handles Prometheus remote read requests.
func NewRemoteReadHandler(q storage.SampleAndChunkQueryable, logger log.Logger, remoteReadMaxBytesInFrame int) http.Handler {
	return &remoteReadHandler{
		logger:    logger,
		queryable: q,
	}
}

func (h *remoteReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req client.ReadRequest
	logger := util_log.WithContext(r.Context(), h.logger)
	if err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRemoteReadQuerySize, &req, util.RawSnappy); err != nil {
		_ = level.Error(logger).Log("msg", "failed to parse proto", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	responseType, err := NegotiateResponseType(req.AcceptedResponseTypes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch responseType {
	case client.STREAMED_XOR_CHUNKS:
		h.remoteReadStreamedXORChunks(ctx, req, w)
	default:
		h.remoteReadSamples(ctx, req, w)
	}
}

func (h *remoteReadHandler) remoteReadStreamedXORChunks(ctx context.Context, req client.ReadRequest, w http.ResponseWriter) {
	w.Header().Add("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")

	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "internal http.ResponseWriter does not implement http.Flusher interface", http.StatusInternalServerError)
		return
	}
	errors := make(chan error)
	for i, qr := range req.Queries {
		go func(i int, qr *client.QueryRequest) {
			from, to, matchers, err := client.FromQueryRequest(qr)
			if err != nil {
				errors <- err
				return
			}

			querier, err := h.queryable.ChunkQuerier(ctx, int64(from), int64(to))
			if err != nil {
				errors <- err
				return
			}

			params := &storage.SelectHints{
				Start: int64(from),
				End:   int64(to),
			}
			ws, err := StreamChunkedReadResponses(
				remote.NewChunkedWriter(w, f),
				int64(i),
				// The streaming API has to provide the series sorted.
				querier.Select(true, params, matchers...),
				h.remoteReadMaxBytesInFrame,
			)
			if err != nil {
				errors <- err
			}

			for _, w := range ws {
				level.Warn(h.logger).Log("msg", "Warnings on chunked remote read query", "warnings", w.Error())
			}
		}(i, qr)
	}

	var lastErr error
	for range req.Queries {
		err := <-errors
		if err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		http.Error(w, lastErr.Error(), http.StatusBadRequest)
		return
	}
}

func (h *remoteReadHandler) remoteReadSamples(ctx context.Context, req client.ReadRequest, w http.ResponseWriter) {
	// Fetch samples for all queries in parallel.
	resp := client.ReadResponse{
		Results: make([]*client.QueryResponse, len(req.Queries)),
	}
	errors := make(chan error)
	for i, qr := range req.Queries {
		go func(i int, qr *client.QueryRequest) {
			from, to, matchers, err := client.FromQueryRequest(qr)
			if err != nil {
				errors <- err
				return
			}

			querier, err := h.queryable.Querier(ctx, int64(from), int64(to))
			if err != nil {
				errors <- err
				return
			}

			params := &storage.SelectHints{
				Start: int64(from),
				End:   int64(to),
			}
			seriesSet := querier.Select(false, params, matchers...)
			resp.Results[i], err = seriesSetToQueryResponse(seriesSet)
			errors <- err
		}(i, qr)
	}

	var lastErr error
	for range req.Queries {
		err := <-errors
		if err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		http.Error(w, lastErr.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Add("Content-Type", "application/x-protobuf")
	if err := util.SerializeProtoResponse(w, &resp, util.RawSnappy); err != nil {
		_ = level.Error(h.logger).Log("msg", "error sending remote read response", "err", err)
	}
}

func seriesSetToQueryResponse(s storage.SeriesSet) (*client.QueryResponse, error) {
	result := &client.QueryResponse{}

	for s.Next() {
		series := s.At()
		samples := []cortexpb.Sample{}
		it := series.Iterator()
		for it.Next() {
			t, v := it.At()
			samples = append(samples, cortexpb.Sample{
				TimestampMs: t,
				Value:       v,
			})
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
		result.Timeseries = append(result.Timeseries, cortexpb.TimeSeries{
			Labels:  cortexpb.FromLabelsToLabelAdapters(series.Labels()),
			Samples: samples,
		})
	}

	return result, s.Err()
}

// NegotiateResponseType returns first accepted response type that this server supports.
// On the empty accepted list we assume that the SAMPLES response type was requested. This is to maintain backward compatibility.
func NegotiateResponseType(accepted []client.ReadRequest_ResponseType) (client.ReadRequest_ResponseType, error) {
	if len(accepted) == 0 {
		accepted = []client.ReadRequest_ResponseType{client.SAMPLES}
	}

	supported := map[client.ReadRequest_ResponseType]struct{}{
		client.SAMPLES:             {},
		client.STREAMED_XOR_CHUNKS: {},
	}

	for _, resType := range accepted {
		if _, ok := supported[resType]; ok {
			return resType, nil
		}
	}
	return 0, errors.Errorf("server does not support any of the requested response types: %v; supported: %v", accepted, supported)
}

// StreamChunkedReadResponses iterates over series, builds chunks and streams those to the caller.
// It expects Series set with populated chunks.
func StreamChunkedReadResponses(
	stream io.Writer,
	queryIndex int64,
	ss storage.ChunkSeriesSet,
	maxBytesInFrame int,
) (storage.Warnings, error) {
	var (
		chks []client.Chunk
		lbls []cortexpb.LabelAdapter
	)

	for ss.Next() {
		series := ss.At()
		iter := series.Iterator()
		lbls = cortexpb.FromLabelsToLabelAdapters(series.Labels())

		frameBytesLeft := maxBytesInFrame
		for _, lbl := range lbls {
			frameBytesLeft -= lbl.Size()
		}

		isNext := iter.Next()

		// Send at most one series per frame; series may be split over multiple frames according to maxBytesInFrame.
		for isNext {
			chk := iter.At()

			if chk.Chunk == nil {
				return ss.Warnings(), errors.Errorf("StreamChunkedReadResponses: found not populated chunk returned by SeriesSet at ref: %v", chk.Ref)
			}

			// Cut the chunk.
			chks = append(chks, client.Chunk{
				StartTimestampMs: chk.MinTime,
				EndTimestampMs: chk.MaxTime,
				Encoding:      int32(chk.Chunk.Encoding()),
				Data:      chk.Chunk.Bytes(),
			})
			frameBytesLeft -= chks[len(chks)-1].Size()

			// We are fine with minor inaccuracy of max bytes per frame. The inaccuracy will be max of full chunk size.
			isNext = iter.Next()
			if frameBytesLeft > 0 && isNext {
				continue
			}

			b, err := proto.Marshal(&client.ChunkedReadResponse{
				ChunkedSeries: []*client.TimeSeriesChunk{
					{Labels: lbls, Chunks: chks},
				},
				QueryIndex: queryIndex,
			})
			if err != nil {
				return ss.Warnings(), errors.Wrap(err, "marshal ChunkedReadResponse")
			}

			if _, err := stream.Write(b); err != nil {
				return ss.Warnings(), errors.Wrap(err, "write to stream")
			}
			chks = chks[:0]
		}
		if err := iter.Err(); err != nil {
			return ss.Warnings(), err
		}
	}
	return ss.Warnings(), ss.Err()
}
