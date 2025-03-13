// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/execution/warnings"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/ringbuffer"
)

type matrixScanner struct {
	labels    labels.Labels
	signature uint64

	buffer           ringbuffer.Buffer
	iterator         chunkenc.Iterator
	lastSample       ringbuffer.Sample
	metricAppearedTs *int64
}

type matrixSelector struct {
	telemetry.OperatorTelemetry

	vectorPool *model.VectorPool
	storage    SeriesSelector
	scalarArg  float64
	scalarArg2 float64
	scanners   []matrixScanner
	series     []labels.Labels
	once       sync.Once

	functionName string
	call         ringbuffer.FunctionCall
	fhReader     *histogram.FloatHistogram
	opts         *query.Options

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

	nonCounterMetric string
	hasFloats        bool
}

var ErrNativeHistogramsNotSupported = errors.New("native histograms are not supported in extended range functions")

// NewMatrixSelector creates operator which selects vector of series over time.
func NewMatrixSelector(
	pool *model.VectorPool,
	selector SeriesSelector,
	functionName string,
	arg float64,
	arg2 float64,
	opts *query.Options,
	selectRange, offset time.Duration,
	batchSize int64,
	shard, numShard int,
) (model.VectorOperator, error) {
	call, err := ringbuffer.NewRangeVectorFunc(functionName)
	if err != nil {
		return nil, err
	}
	m := &matrixSelector{
		storage:      selector,
		call:         call,
		functionName: functionName,
		vectorPool:   pool,
		scalarArg:    arg,
		scalarArg2:   arg2,
		fhReader:     &histogram.FloatHistogram{},

		opts:          opts,
		numSteps:      opts.NumSteps(),
		mint:          opts.Start.UnixMilli(),
		maxt:          opts.End.UnixMilli(),
		step:          opts.Step.Milliseconds(),
		isExtFunction: parse.IsExtFunction(functionName),

		selectRange:     selectRange.Milliseconds(),
		offset:          offset.Milliseconds(),
		currentStep:     opts.Start.UnixMilli(),
		seriesBatchSize: batchSize,

		shard:     shard,
		numShards: numShard,

		extLookbackDelta: opts.ExtLookbackDelta.Milliseconds(),
	}
	m.OperatorTelemetry = telemetry.NewTelemetry(m, opts)

	// For instant queries, set the step to a positive value
	// so that the operator can terminate.
	if m.step == 0 {
		m.step = 1
	}

	return m, nil
}

func (o *matrixSelector) Explain() []model.VectorOperator {
	return nil
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
		if o.nonCounterMetric != "" && o.hasFloats {
			warnings.AddToContext(annotations.NewPossibleNonCounterInfo(o.nonCounterMetric, posrange.PositionRange{}), ctx)
		}

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
			scanner  = &o.scanners[o.currentSeries]
			seriesTs = ts
		)

		for currStep := 0; currStep < o.numSteps && seriesTs <= o.maxt; currStep++ {
			maxt := seriesTs - o.offset
			mint := maxt - o.selectRange

			if err := scanner.selectPoints(mint, maxt, seriesTs, o.fhReader, o.isExtFunction); err != nil {
				return nil, err
			}
			// TODO(saswatamcode): Handle multi-arg functions for matrixSelectors.
			// Also, allow operator to exist independently without being nested
			// under parser.Call by implementing new data model.
			// https://github.com/thanos-io/promql-engine/issues/39
			f, h, ok, err := scanner.buffer.Eval(ctx, o.scalarArg, o.scalarArg2, scanner.metricAppearedTs)
			if err != nil {
				return nil, err
			}
			if ok {
				vectors[currStep].T = seriesTs
				if h != nil {
					vectors[currStep].AppendHistogram(o.vectorPool, scanner.signature, h)
				} else {
					vectors[currStep].AppendSample(o.vectorPool, scanner.signature, f)
					o.hasFloats = true
				}
			}
			o.IncrementSamplesAtTimestamp(scanner.buffer.Len(), seriesTs)
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
				lastSample: ringbuffer.Sample{T: math.MinInt64},
				buffer:     o.newBuffer(ctx),
			}
			o.series[i] = lbls
		}
		numSeries := int64(len(o.series))
		if o.seriesBatchSize == 0 || numSeries < o.seriesBatchSize {
			o.seriesBatchSize = numSeries
		}
		o.vectorPool.SetStepSize(int(o.seriesBatchSize))

		// Add a warning if rate or increase is applied on metrics which are not named like counters.
		if o.functionName == "rate" || o.functionName == "increase" {
			if len(series) > 0 {
				metricName := series[0].Labels().Get(labels.MetricName)
				if metricName != "" &&
					!strings.HasSuffix(metricName, "_total") &&
					!strings.HasSuffix(metricName, "_sum") &&
					!strings.HasSuffix(metricName, "_count") &&
					!strings.HasSuffix(metricName, "_bucket") {
					o.nonCounterMetric = metricName
				}
			}
		}
	})
	return err
}

func (o *matrixSelector) newBuffer(ctx context.Context) ringbuffer.Buffer {
	switch o.functionName {
	case "rate":
		return ringbuffer.NewRateBuffer(ctx, *o.opts, true, true, o.selectRange, o.offset)
	case "increase":
		return ringbuffer.NewRateBuffer(ctx, *o.opts, true, false, o.selectRange, o.offset)
	case "delta":
		return ringbuffer.NewRateBuffer(ctx, *o.opts, false, false, o.selectRange, o.offset)
	}

	if o.isExtFunction {
		return ringbuffer.NewWithExtLookback(ctx, 8, o.selectRange, o.offset, o.opts.ExtLookbackDelta.Milliseconds()-1, o.call)
	}
	return ringbuffer.New(ctx, 8, o.selectRange, o.offset, o.call)

}

func (o *matrixSelector) String() string {
	r := time.Duration(o.selectRange) * time.Millisecond
	if o.call != nil {
		return fmt.Sprintf("[matrixSelector] %v({%v}[%s] %v mod %v)", o.functionName, o.storage.Matchers(), r, o.shard, o.numShards)
	}
	return fmt.Sprintf("[matrixSelector] {%v}[%s] %v mod %v", o.storage.Matchers(), r, o.shard, o.numShards)
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
func (m *matrixScanner) selectPoints(
	mint, maxt, evalt int64,
	fh *histogram.FloatHistogram,
	isExtFunction bool,
) error {
	m.buffer.Reset(mint, evalt)
	if m.lastSample.T > maxt {
		return nil
	}

	if bufMaxt := m.buffer.MaxT() + 1; bufMaxt > mint {
		mint = bufMaxt
	}
	mint = maxInt64(mint, m.buffer.MaxT()+1)
	if m.lastSample.T > mint {
		m.buffer.Push(m.lastSample.T, m.lastSample.V)
		m.lastSample.T = math.MinInt64
		mint = maxInt64(mint, m.buffer.MaxT()+1)
	}

	appendedPointBeforeMint := m.buffer.Len() > 0
	for valType := m.iterator.Next(); valType != chunkenc.ValNone; valType = m.iterator.Next() {
		switch valType {
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			if isExtFunction {
				return ErrNativeHistogramsNotSupported
			}
			var t int64
			t, fh = m.iterator.AtFloatHistogram(fh)
			if value.IsStaleNaN(fh.Sum) || t < mint {
				continue
			}
			if t > maxt {
				m.lastSample.T = t
				if m.lastSample.V.H == nil {
					m.lastSample.V.H = fh.Copy()
				} else {
					fh.CopyTo(m.lastSample.V.H)
				}
				return nil
			}
			if t > mint {
				m.buffer.Push(t, ringbuffer.Value{H: fh})
			}
		case chunkenc.ValFloat:
			t, v := m.iterator.At()
			if value.IsStaleNaN(v) {
				continue
			}
			if m.metricAppearedTs == nil {
				tCopy := t
				m.metricAppearedTs = &tCopy
			}
			if t > maxt {
				m.lastSample.T, m.lastSample.V.F, m.lastSample.V.H = t, v, nil
				return nil
			}
			if isExtFunction {
				if t > mint || !appendedPointBeforeMint {
					m.buffer.Push(t, ringbuffer.Value{F: v})
					appendedPointBeforeMint = true
				} else {
					m.buffer.ReadIntoLast(func(s *ringbuffer.Sample) {
						s.T, s.V.F, s.V.H = t, v, nil
					})
				}
			} else {
				if t > mint {
					m.buffer.Push(t, ringbuffer.Value{F: v})
				}
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
