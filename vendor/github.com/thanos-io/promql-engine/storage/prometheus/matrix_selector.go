// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/promql-engine/execution/function"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/scan"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/ringbuffer"
)

type matrixScanner struct {
	labels    labels.Labels
	signature uint64

	buffer           *ringbuffer.RingBuffer[scan.Value]
	iterator         chunkenc.Iterator
	lastSample       ringbuffer.Sample[scan.Value]
	metricAppearedTs *int64
}

type matrixSelector struct {
	model.OperatorTelemetry

	vectorPool   *model.VectorPool
	functionName string
	storage      SeriesSelector
	scalarArgs   []float64
	call         scan.FunctionCall
	scanners     []matrixScanner
	bufferTail   []ringbuffer.Sample[scan.Value]
	series       []labels.Labels
	once         sync.Once

	numSteps      int
	mint          int64
	maxt          int64
	step          int64
	selectRange   int64
	offset        int64
	isExtFunction bool

	currentStep     int64
	currentSeries   int64
	seriesBatchSize int64

	shard     int
	numShards int

	// Lookback delta for extended range functions.
	extLookbackDelta int64
}

var ErrNativeHistogramsNotSupported = errors.New("native histograms are not supported in extended range functions")

// NewMatrixSelector creates operator which selects vector of series over time.
func NewMatrixSelector(
	pool *model.VectorPool,
	selector SeriesSelector,
	functionName string,
	arg float64,
	opts *query.Options,
	selectRange, offset time.Duration,
	batchSize int64,
	shard, numShard int,
) (model.VectorOperator, error) {
	call, err := scan.NewRangeVectorFunc(functionName)
	if err != nil {
		return nil, err
	}
	isExtFunction := function.IsExtFunction(functionName)
	m := &matrixSelector{
		OperatorTelemetry: model.NewTelemetry("matrixSelector", opts.EnableAnalysis),

		storage:      selector,
		call:         call,
		functionName: functionName,
		vectorPool:   pool,
		scalarArgs:   []float64{arg},
		bufferTail:   make([]ringbuffer.Sample[scan.Value], 16),

		numSteps:      opts.NumSteps(),
		mint:          opts.Start.UnixMilli(),
		maxt:          opts.End.UnixMilli(),
		step:          opts.Step.Milliseconds(),
		isExtFunction: isExtFunction,

		selectRange:     selectRange.Milliseconds(),
		offset:          offset.Milliseconds(),
		currentStep:     opts.Start.UnixMilli(),
		seriesBatchSize: batchSize,

		shard:     shard,
		numShards: numShard,

		extLookbackDelta: opts.ExtLookbackDelta.Milliseconds(),
	}

	// For instant queries, set the step to a positive value
	// so that the operator can terminate.
	if m.step == 0 {
		m.step = 1
	}

	return m, nil
}

func (o *matrixSelector) Explain() (me string, next []model.VectorOperator) {
	r := time.Duration(o.selectRange) * time.Millisecond
	if o.call != nil {
		return fmt.Sprintf("[matrixSelector] %v({%v}[%s] %v mod %v)", o.functionName, o.storage.Matchers(), r, o.shard, o.numShards), nil
	}
	return fmt.Sprintf("[matrixSelector] {%v}[%s] %v mod %v", o.storage.Matchers(), r, o.shard, o.numShards), nil
}

func (o *matrixSelector) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *matrixSelector) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *matrixSelector) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if o.currentStep > o.maxt {
		return nil, nil
	}
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}

	ts := o.currentStep
	vectors := o.vectorPool.GetVectorBatch()
	for currStep := 0; currStep < o.numSteps && ts <= o.maxt; currStep++ {
		vectors = append(vectors, o.vectorPool.GetStepVector(ts))
		ts += o.step
	}

	// Reset the current timestamp.
	ts = o.currentStep
	firstSeries := o.currentSeries
	for ; o.currentSeries-firstSeries < o.seriesBatchSize && o.currentSeries < int64(len(o.scanners)); o.currentSeries++ {
		var (
			series   = &o.scanners[o.currentSeries]
			seriesTs = ts
		)

		for currStep := 0; currStep < o.numSteps && seriesTs <= o.maxt; currStep++ {
			maxt := seriesTs - o.offset
			mint := maxt - o.selectRange

			var err error

			if !o.isExtFunction {
				err = series.selectPoints(mint, maxt)
			} else {
				err = series.selectExtPoints(mint, maxt, o.extLookbackDelta)
			}
			if err != nil {
				return nil, err
			}

			// TODO(saswatamcode): Handle multi-arg functions for matrixSelectors.
			// Also, allow operator to exist independently without being nested
			// under parser.Call by implementing new data model.
			// https://github.com/thanos-io/promql-engine/issues/39
			f, h, ok := o.call(scan.FunctionArgs{
				Samples:          series.buffer.Samples(),
				StepTime:         seriesTs,
				SelectRange:      o.selectRange,
				Offset:           o.offset,
				ScalarPoints:     o.scalarArgs,
				MetricAppearedTs: series.metricAppearedTs,
			})

			if ok {
				vectors[currStep].T = seriesTs
				if h != nil {
					vectors[currStep].AppendHistogram(o.vectorPool, series.signature, h)
				} else {
					vectors[currStep].AppendSample(o.vectorPool, series.signature, f)
				}
			}
			seriesTs += o.step
		}
	}
	if o.currentSeries == int64(len(o.scanners)) {
		o.currentStep += o.step * int64(o.numSteps)
		o.currentSeries = 0
	}
	return vectors, nil
}

func (o *matrixSelector) loadSeries(ctx context.Context) error {
	var err error
	o.once.Do(func() {
		series, loadErr := o.storage.GetSeries(ctx, o.shard, o.numShards)
		if loadErr != nil {
			err = loadErr
			return
		}

		o.scanners = make([]matrixScanner, len(series))
		o.series = make([]labels.Labels, len(series))
		b := labels.ScratchBuilder{}
		for i, s := range series {
			lbls := s.Labels()
			if o.functionName != "last_over_time" {
				// This modifies the array in place. Because labels.Labels
				// can be re-used between different Select() calls, it means that
				// we have to copy it here.
				// TODO(GiedriusS): could we identify somehow whether labels.Labels
				// is reused between Select() calls?
				lbls, _ = extlabels.DropMetricName(lbls, b)
			}
			o.scanners[i] = matrixScanner{
				labels:     lbls,
				signature:  s.Signature,
				iterator:   s.Iterator(nil),
				lastSample: ringbuffer.Sample[scan.Value]{T: math.MinInt64},
				buffer:     ringbuffer.New[scan.Value](8),
			}
			o.series[i] = lbls
		}
		numSeries := int64(len(o.series))
		if o.seriesBatchSize == 0 || numSeries < o.seriesBatchSize {
			o.seriesBatchSize = numSeries
		}
		o.vectorPool.SetStepSize(int(o.seriesBatchSize))
	})
	return err
}

// matrixIterSlice populates a matrix vector covering the requested range for a
// single time series, with points retrieved from an iterator.
//
// As an optimization, the matrix vector may already contain points of the same
// time series from the evaluation of an earlier step (with lower mint and maxt
// values). Any such points falling before mint are discarded; points that fall
// into the [mint, maxt] range are retained; only points with later timestamps
// are populated from the iterator.
// TODO(fpetkovski): Add max samples limit.
func (m *matrixScanner) selectPoints(mint, maxt int64) error {
	m.buffer.DropBefore(mint)
	if m.lastSample.T > maxt {
		return nil
	}

	mint = maxInt64(mint, m.buffer.MaxT()+1)
	if m.lastSample.T >= mint {
		m.buffer.ReadIntoNext(func(s *ringbuffer.Sample[scan.Value]) bool {
			s.T, s.V.F = m.lastSample.T, m.lastSample.V.F
			if m.lastSample.V.H != nil {
				if s.V.H == nil {
					s.V.H = m.lastSample.V.H.Copy()
				} else {
					m.lastSample.V.H.CopyTo(s.V.H)
				}
			}
			return true
		})
		m.lastSample.T = math.MinInt64
		mint = maxInt64(mint, m.buffer.MaxT()+1)
	}

	for valType := m.iterator.Next(); valType != chunkenc.ValNone; valType = m.iterator.Next() {
		switch valType {
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			var stop bool
			m.buffer.ReadIntoNext(func(s *ringbuffer.Sample[scan.Value]) (keep bool) {
				if s.V.H == nil {
					s.V.H = &histogram.FloatHistogram{}
				}
				s.T, s.V.H = m.iterator.AtFloatHistogram(s.V.H)
				if value.IsStaleNaN(s.V.H.Sum) {
					return false
				}
				if s.T < mint {
					return false
				}
				if s.T > maxt {
					m.lastSample.T, m.lastSample.V.H = s.T, s.V.H.Copy()
					stop = true
					return false
				}
				return true
			})
			if stop {
				return nil
			}
		case chunkenc.ValFloat:
			t, v := m.iterator.At()
			if value.IsStaleNaN(v) {
				continue
			}
			if t > maxt {
				m.lastSample.T, m.lastSample.V.F, m.lastSample.V.H = t, v, nil
				return nil
			}
			if t >= mint {
				m.buffer.Push(t, scan.Value{F: v})
			}
		}
	}
	return m.iterator.Err()
}

// matrixIterSlice populates a matrix vector covering the requested range for a
// single time series, with points retrieved from an iterator.
//
// As an optimization, the matrix vector may already contain points of the same
// time series from the evaluation of an earlier step (with lower mint and maxt
// values). Any such points falling before mint are discarded; points that fall
// into the [mint, maxt] range are retained; only points with later timestamps
// are populated from the iterator.
// TODO(fpetkovski): Add max samples limit.
func (m *matrixScanner) selectExtPoints(mint, maxt, extLookbackDelta int64) error {
	m.buffer.DropBeforeWithExtLookback(mint, mint-extLookbackDelta)
	if m.lastSample.T > maxt {
		return nil
	}

	mint = maxInt64(mint, m.buffer.MaxT()+1)
	if m.lastSample.T >= mint {
		m.buffer.Push(m.lastSample.T, scan.Value{F: m.lastSample.V.F, H: m.lastSample.V.H})
		m.lastSample.T = math.MinInt64
		mint = maxInt64(m.buffer.MaxT()+1, mint)
	}

	appendedPointBeforeMint := m.buffer.Len() > 0
	for valType := m.iterator.Next(); valType != chunkenc.ValNone; valType = m.iterator.Next() {
		switch valType {
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			return ErrNativeHistogramsNotSupported
		case chunkenc.ValFloat:
			t, v := m.iterator.At()
			if value.IsStaleNaN(v) {
				continue
			}
			if m.metricAppearedTs == nil {
				m.metricAppearedTs = &t
			}
			if t > maxt {
				m.lastSample.T, m.lastSample.V.F, m.lastSample.V.H = t, v, nil
				return nil
			}
			if t >= mint || !appendedPointBeforeMint {
				m.buffer.Push(t, scan.Value{F: v})
				appendedPointBeforeMint = true
			} else {
				m.buffer.ReadIntoLast(func(s *ringbuffer.Sample[scan.Value]) {
					s.T, s.V.F, s.V.H = t, v, nil
				})
			}
		}
	}
	return m.iterator.Err()
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b

}
