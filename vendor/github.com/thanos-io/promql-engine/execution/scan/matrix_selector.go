// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/promql-engine/execution/function"
	"github.com/thanos-io/promql-engine/execution/model"
	engstore "github.com/thanos-io/promql-engine/execution/storage"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"
)

type matrixScanner struct {
	labels           labels.Labels
	signature        uint64
	previousSamples  []Sample
	samples          *storage.BufferedSeriesIterator
	metricAppearedTs *int64
	deltaReduced     bool
}

type matrixSelector struct {
	vectorPool   *model.VectorPool
	functionName string
	storage      engstore.SeriesSelector
	scalarArgs   []float64
	call         FunctionCall
	scanners     []matrixScanner
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
	model.OperatorTelemetry
}

var ErrNativeHistogramsNotSupported = errors.New("native histograms are not supported in extended range functions")

// NewMatrixSelector creates operator which selects vector of series over time.
func NewMatrixSelector(
	pool *model.VectorPool,
	selector engstore.SeriesSelector,
	functionName string,
	arg float64,
	opts *query.Options,
	selectRange, offset time.Duration,
	batchSize int64,
	shard, numShard int,
) (model.VectorOperator, error) {
	call, err := NewRangeVectorFunc(functionName)
	if err != nil {
		return nil, err
	}
	isExtFunction := function.IsExtFunction(functionName)
	m := &matrixSelector{
		storage:      selector,
		call:         call,
		functionName: functionName,
		vectorPool:   pool,
		scalarArgs:   []float64{arg},

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
	m.OperatorTelemetry = &model.NoopTelemetry{}
	if opts.EnableAnalysis {
		m.OperatorTelemetry = &model.TrackedTelemetry{}
	}
	// For instant queries, set the step to a positive value
	// so that the operator can terminate.
	if m.step == 0 {
		m.step = 1
	}

	return m, nil
}

func (o *matrixSelector) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	o.SetName("[*matrixSelector]")
	return o, nil
}

func (o *matrixSelector) Explain() (me string, next []model.VectorOperator) {
	r := time.Duration(o.selectRange) * time.Millisecond
	if o.call != nil {
		return fmt.Sprintf("[*matrixSelector] %v({%v}[%s] %v mod %v)", o.functionName, o.storage.Matchers(), r, o.shard, o.numShards), nil
	}
	return fmt.Sprintf("[*matrixSelector] {%v}[%s] %v mod %v", o.storage.Matchers(), r, o.shard, o.numShards), nil
}

func (o *matrixSelector) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *matrixSelector) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *matrixSelector) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

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

			var rangeSamples []Sample
			var err error

			if !o.isExtFunction {
				rangeSamples, err = selectPoints(series.samples, mint, maxt, series.previousSamples)
			} else {
				rangeSamples, err = selectExtPoints(series.samples, mint, maxt, series.previousSamples, o.extLookbackDelta, &series.metricAppearedTs)
			}

			if err != nil {
				return nil, err
			}

			// TODO(saswatamcode): Handle multi-arg functions for matrixSelectors.
			// Also, allow operator to exist independently without being nested
			// under parser.Call by implementing new data model.
			// https://github.com/thanos-io/promql-engine/issues/39
			f, h, ok := o.call(FunctionArgs{
				Samples:          rangeSamples,
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

			series.previousSamples = rangeSamples

			// Only buffer bufferRange milliseconds from the second step on.
			bufferRange := o.selectRange
			if bufferRange > o.step {
				bufferRange = o.step
			}
			if !series.deltaReduced {
				series.samples.ReduceDelta(bufferRange)
				series.deltaReduced = true
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

			// If we are dealing with an extended range function we need to search further in the past for valid series.
			var selectRange = o.selectRange
			if o.isExtFunction {
				selectRange += o.extLookbackDelta
			}

			o.scanners[i] = matrixScanner{
				labels:       lbls,
				signature:    s.Signature,
				samples:      storage.NewBufferIterator(s.Iterator(nil), selectRange),
				deltaReduced: o.isExtFunction,
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
func selectPoints(it *storage.BufferedSeriesIterator, mint, maxt int64, out []Sample) ([]Sample, error) {
	if len(out) > 0 && out[len(out)-1].T >= mint {
		// There is an overlap between previous and current ranges, retain common
		// points. In most such cases:
		//   (a) the overlap is significantly larger than the eval step; and/or
		//   (b) the number of samples is relatively small.
		// so a linear search will be as fast as a binary search.
		var drop int
		for drop = 0; out[drop].T < mint; drop++ {
		}
		copy(out, out[drop:])
		out = out[:len(out)-drop]
		// Only append points with timestamps after the last timestamp we have.
		mint = out[len(out)-1].T + 1
	} else {
		out = out[:0]
	}

	soughtValueType := it.Seek(maxt)
	if soughtValueType == chunkenc.ValNone {
		if it.Err() != nil {
			return nil, it.Err()
		}
	}

	buf := it.Buffer()
loop:
	for {
		switch buf.Next() {
		case chunkenc.ValNone:
			break loop
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			t, fh := buf.AtFloatHistogram()
			if value.IsStaleNaN(fh.Sum) {
				continue loop
			}
			if t >= mint {
				n := len(out)
				if cap(out) > n {
					out = out[:len(out)+1]
				} else {
					out = append(out, Sample{})
				}
				out[n].T, out[n].H = t, fh
			}
		case chunkenc.ValFloat:
			t, v := buf.At()
			if value.IsStaleNaN(v) {
				continue loop
			}
			// Values in the buffer are guaranteed to be smaller than maxt.
			if t >= mint {
				n := len(out)
				if cap(out) > n {
					out = out[:len(out)+1]
				} else {
					out = append(out, Sample{})
				}
				out[n].T, out[n].F, out[n].H = t, v, nil
			}
		}
	}

	// The sought sample might also be in the range.
	switch soughtValueType {
	case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
		t, fh := it.AtFloatHistogram()
		if t == maxt && !value.IsStaleNaN(fh.Sum) {
			n := len(out)
			if cap(out) > n {
				out = out[:len(out)+1]
			} else {
				out = append(out, Sample{})
			}
			out[n].T, out[n].H = t, fh
		}
	case chunkenc.ValFloat:
		t, v := it.At()
		if t == maxt && !value.IsStaleNaN(v) {
			n := len(out)
			if cap(out) > n {
				out = out[:len(out)+1]
			} else {
				out = append(out, Sample{})
			}
			out[n].T, out[n].F, out[n].H = t, v, nil
		}
	}

	return out, nil
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
func selectExtPoints(it *storage.BufferedSeriesIterator, mint, maxt int64, out []Sample, extLookbackDelta int64, metricAppearedTs **int64) ([]Sample, error) {
	extMint := mint - extLookbackDelta
	selectsNativeHistograms := false

	if len(out) > 0 && out[len(out)-1].T >= mint {
		// There is an overlap between previous and current ranges, retain common
		// points. In most such cases:
		//   (a) the overlap is significantly larger than the eval step; and/or
		//   (b) the number of samples is relatively small.
		// so a linear search will be as fast as a binary search.
		var drop int

		// This is an argument to an extended range function, first go past mint.
		for drop = 0; drop < len(out) && out[drop].T <= mint; drop++ {

		}
		// Then, go back one sample if within lookbackDelta of mint.
		if drop > 0 && out[drop-1].T >= extMint {
			drop--
		}
		if out[len(out)-1].T >= mint {
			// Only append points with timestamps after the last timestamp we have.
			mint = out[len(out)-1].T + 1
		}

		copy(out, out[drop:])
		out = out[:len(out)-drop]
	} else {
		out = out[:0]
	}

	soughtValueType := it.Seek(maxt)
	if soughtValueType == chunkenc.ValNone {
		if it.Err() != nil {
			return nil, it.Err()
		}
	}

	appendedPointBeforeMint := len(out) > 0
	buf := it.Buffer()
loop:
	for {
		switch buf.Next() {
		case chunkenc.ValNone:
			break loop
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			selectsNativeHistograms = true
			t, fh := buf.AtFloatHistogram()
			if value.IsStaleNaN(fh.Sum) {
				continue loop
			}
			if *metricAppearedTs == nil {
				*metricAppearedTs = &t
			}
			if t >= mint {
				out = append(out, Sample{T: t, H: fh})
			}
		case chunkenc.ValFloat:
			t, v := buf.At()
			if value.IsStaleNaN(v) {
				continue loop
			}
			if *metricAppearedTs == nil {
				*metricAppearedTs = &t
			}

			// This is the argument to an extended range function: if any point
			// exists at or before range start, add it and then keep replacing
			// it with later points while not yet (strictly) inside the range.
			if t >= mint || !appendedPointBeforeMint {
				out = append(out, Sample{T: t, F: v})
				appendedPointBeforeMint = true
			} else {
				out[len(out)-1] = Sample{T: t, F: v}
			}

		}
	}

	// The sought sample might also be in the range.
	switch soughtValueType {
	case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
		selectsNativeHistograms = true
		t, fh := it.AtFloatHistogram()
		if t == maxt && !value.IsStaleNaN(fh.Sum) {
			if *metricAppearedTs == nil {
				*metricAppearedTs = &t
			}
			out = append(out, Sample{T: t, H: fh})
		}
	case chunkenc.ValFloat:
		t, v := it.At()
		if t == maxt && !value.IsStaleNaN(v) {
			if *metricAppearedTs == nil {
				*metricAppearedTs = &t
			}
			out = append(out, Sample{T: t, F: v})
		}
	}

	if selectsNativeHistograms {
		return nil, ErrNativeHistogramsNotSupported
	}

	return out, nil
}
