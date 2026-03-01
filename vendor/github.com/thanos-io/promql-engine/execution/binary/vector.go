// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"context"
	"fmt"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/cespare/xxhash/v2"
	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"
)

type joinBucket struct {
	ats, bts     int64
	sid          uint64
	val          float64
	histogramVal *histogram.FloatHistogram
}

// vectorOperator evaluates an expression between two step vectors.
type vectorOperator struct {
	lhs        model.VectorOperator
	rhs        model.VectorOperator
	matching   *parser.VectorMatching
	opType     parser.ItemType
	returnBool bool
	stepsBatch int
	sigFunc    func(labels.Labels) uint64

	once         sync.Once
	series       []labels.Labels
	lhsSampleIDs []labels.Labels
	rhsSampleIDs []labels.Labels
	outputMap    map[uint64]uint64

	lcJoinBuckets []*joinBucket
	hcJoinBuckets []*joinBucket

	lhsBuf []model.StepVector
	rhsBuf []model.StepVector
}

func NewVectorOperator(
	lhs model.VectorOperator,
	rhs model.VectorOperator,
	matching *parser.VectorMatching,
	opType parser.ItemType,
	returnBool bool,
	opts *query.Options,
) (model.VectorOperator, error) {
	op := &vectorOperator{
		lhs:        lhs,
		rhs:        rhs,
		matching:   matching,
		opType:     opType,
		returnBool: returnBool,
		sigFunc:    signatureFunc(matching.On, matching.MatchingLabels...),
		stepsBatch: opts.StepsBatch,
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(op, opts), op), nil
}

func (o *vectorOperator) String() string {
	if o.matching.On {
		return fmt.Sprintf("[vectorBinary] %s - %v, on: %v, group: %v", parser.ItemTypeStr[o.opType], o.matching.Card.String(), o.matching.MatchingLabels, o.matching.Include)
	}
	return fmt.Sprintf("[vectorBinary] %s - %v, ignoring: %v, group: %v", parser.ItemTypeStr[o.opType], o.matching.Card.String(), o.matching.On, o.matching.Include)
}

func (o *vectorOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.lhs, o.rhs}
}

func (o *vectorOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.initOnce(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *vectorOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	// Some operators do not call Series of all their children.
	if err := o.initOnce(ctx); err != nil {
		return 0, err
	}

	var lhsN int
	var lerrChan = make(chan error, 1)
	go func() {
		var err error
		lhsN, err = o.lhs.Next(ctx, o.lhsBuf)
		if err != nil {
			lerrChan <- err
		}
		close(lerrChan)
	}()

	rhsN, rerr := o.rhs.Next(ctx, o.rhsBuf)
	lerr := <-lerrChan
	if rerr != nil {
		return 0, rerr
	}
	if lerr != nil {
		return 0, lerr
	}

	// TODO(fpetkovski): When one operator becomes empty,
	// we might want to drain or close the other one.
	// We don't have a concept of closing an operator yet.
	if lhsN == 0 || rhsN == 0 {
		return 0, nil
	}

	n := 0
	minN := min(rhsN, lhsN)

	for i := 0; i < minN && n < len(buf); i++ {
		if err := o.execBinaryOperation(ctx, o.lhsBuf[i], o.rhsBuf[i], &buf[n]); err != nil {
			return 0, err
		}
		n++
	}

	return n, nil
}

func (o *vectorOperator) initOnce(ctx context.Context) error {
	var err error
	o.once.Do(func() { err = o.init(ctx) })
	return err
}

func (o *vectorOperator) init(ctx context.Context) error {
	var highCardSide []labels.Labels
	var errChan = make(chan error, 1)
	go func() {
		var err error
		highCardSide, err = o.lhs.Series(ctx)
		if err != nil {
			errChan <- err
		}
		close(errChan)
	}()

	lowCardSide, err := o.rhs.Series(ctx)
	if err != nil {
		return err
	}
	if err := <-errChan; err != nil {
		return err
	}
	o.lhsSampleIDs = highCardSide
	o.rhsSampleIDs = lowCardSide

	if o.matching.Card == parser.CardOneToMany {
		highCardSide, lowCardSide = lowCardSide, highCardSide
	}

	o.initJoinTables(highCardSide, lowCardSide)

	// Pre-allocate buffers with appropriate inner slice capacities
	// based on series counts from each side.
	lhsSeriesCount := len(o.lhsSampleIDs)
	rhsSeriesCount := len(o.rhsSampleIDs)

	o.lhsBuf = make([]model.StepVector, o.stepsBatch)
	o.rhsBuf = make([]model.StepVector, o.stepsBatch)

	// Pre-allocate float sample slices; histogram slices will grow on demand.
	for i := range o.lhsBuf {
		o.lhsBuf[i].SampleIDs = make([]uint64, 0, lhsSeriesCount)
		o.lhsBuf[i].Samples = make([]float64, 0, lhsSeriesCount)
	}
	for i := range o.rhsBuf {
		o.rhsBuf[i].SampleIDs = make([]uint64, 0, rhsSeriesCount)
		o.rhsBuf[i].Samples = make([]float64, 0, rhsSeriesCount)
	}

	return nil
}

func (o *vectorOperator) execBinaryOperation(ctx context.Context, lhs, rhs model.StepVector, step *model.StepVector) error {
	switch o.opType {
	case parser.LAND:
		return o.execBinaryAnd(lhs, rhs, step)
	case parser.LOR:
		return o.execBinaryOr(lhs, rhs, step)
	case parser.LUNLESS:
		return o.execBinaryUnless(lhs, rhs, step)
	default:
		return o.execBinaryArithmetic(ctx, lhs, rhs, step)
	}
}

func (o *vectorOperator) execBinaryAnd(lhs, rhs model.StepVector, step *model.StepVector) error {
	ts := lhs.T
	step.Reset(ts)

	for _, sampleID := range rhs.SampleIDs {
		jp := o.lcJoinBuckets[sampleID]
		jp.ats = ts
	}

	for _, histogramID := range rhs.HistogramIDs {
		jp := o.lcJoinBuckets[histogramID]
		jp.ats = ts
	}

	sampleHint := len(lhs.Samples)
	for i, sampleID := range lhs.SampleIDs {
		if jp := o.hcJoinBuckets[sampleID]; jp.ats == ts {
			step.AppendSampleWithSizeHint(o.outputSeriesID(sampleID+1, 0), lhs.Samples[i], sampleHint)
		}
	}

	histogramHint := len(lhs.Histograms)
	for i, histogramID := range lhs.HistogramIDs {
		if jp := o.hcJoinBuckets[histogramID]; jp.ats == ts {
			step.AppendHistogramWithSizeHint(o.outputSeriesID(histogramID+1, 0), lhs.Histograms[i], histogramHint)
		}
	}
	return nil
}

func (o *vectorOperator) execBinaryOr(lhs, rhs model.StepVector, step *model.StepVector) error {
	ts := lhs.T
	step.Reset(ts)

	sampleHint := len(lhs.Samples) + len(rhs.Samples)
	for i, sampleID := range lhs.SampleIDs {
		jp := o.hcJoinBuckets[sampleID]
		jp.ats = ts
		step.AppendSampleWithSizeHint(o.outputSeriesID(sampleID+1, 0), lhs.Samples[i], sampleHint)
	}

	histogramHint := len(lhs.Histograms) + len(rhs.Histograms)
	for i, histogramID := range lhs.HistogramIDs {
		jp := o.hcJoinBuckets[histogramID]
		jp.ats = ts
		step.AppendHistogramWithSizeHint(o.outputSeriesID(histogramID+1, 0), lhs.Histograms[i], histogramHint)
	}

	for i, sampleID := range rhs.SampleIDs {
		if jp := o.lcJoinBuckets[sampleID]; jp.ats != ts {
			step.AppendSampleWithSizeHint(o.outputSeriesID(0, sampleID+1), rhs.Samples[i], sampleHint)
		}
	}

	for i, histogramID := range rhs.HistogramIDs {
		if jp := o.lcJoinBuckets[histogramID]; jp.ats != ts {
			step.AppendHistogramWithSizeHint(o.outputSeriesID(0, histogramID+1), rhs.Histograms[i], histogramHint)
		}
	}

	return nil
}

func (o *vectorOperator) execBinaryUnless(lhs, rhs model.StepVector, step *model.StepVector) error {
	ts := lhs.T
	step.Reset(ts)

	for _, sampleID := range rhs.SampleIDs {
		jp := o.lcJoinBuckets[sampleID]
		jp.ats = ts
	}
	for _, histogramID := range rhs.HistogramIDs {
		jp := o.lcJoinBuckets[histogramID]
		jp.ats = ts
	}

	sampleHint := len(lhs.Samples)
	for i, sampleID := range lhs.SampleIDs {
		if jp := o.hcJoinBuckets[sampleID]; jp.ats != ts {
			step.AppendSampleWithSizeHint(o.outputSeriesID(sampleID+1, 0), lhs.Samples[i], sampleHint)
		}
	}
	histogramHint := len(lhs.Histograms)
	for i, histogramID := range lhs.HistogramIDs {
		if jp := o.hcJoinBuckets[histogramID]; jp.ats != ts {
			step.AppendHistogramWithSizeHint(o.outputSeriesID(histogramID+1, 0), lhs.Histograms[i], histogramHint)
		}
	}
	return nil
}

func (o *vectorOperator) computeBinaryPairing(hval, lval float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool, warnings.Warnings, error) {
	// operand is not commutative so we need to address potential swapping
	if o.matching.Card == parser.CardOneToMany {
		return binOp(o.opType, lval, hval, hlhs, hrhs)
	}
	return binOp(o.opType, hval, lval, hlhs, hrhs)
}

func (o *vectorOperator) execBinaryArithmetic(ctx context.Context, lhs, rhs model.StepVector, step *model.StepVector) error {
	ts := lhs.T
	step.Reset(ts)

	var (
		hcs, lcs model.StepVector
		h        *histogram.FloatHistogram
		keep     bool
		err      error
	)

	switch o.matching.Card {
	case parser.CardManyToOne, parser.CardOneToOne:
		hcs, lcs = lhs, rhs
	case parser.CardOneToMany:
		hcs, lcs = rhs, lhs
	default:
		return errors.Newf("Unexpected matching cardinality: %s", o.matching.Card.String())
	}

	// shortcut: if we have no samples and histograms on the high card side we cannot compute pairings
	if len(hcs.Samples) == 0 && len(hcs.Histograms) == 0 {
		return nil
	}
	for i, sampleID := range lcs.SampleIDs {
		jp := o.lcJoinBuckets[sampleID]
		// Hash collisions on the low-card-side would imply a many-to-many relation.
		if jp.ats == ts {
			return o.newManyToManyMatchErrorOnLowCardSide(jp.sid, sampleID)
		}
		jp.sid = sampleID
		jp.val = lcs.Samples[i]
		jp.ats = ts
	}

	for i, histogramID := range lcs.HistogramIDs {
		jp := o.lcJoinBuckets[histogramID]
		// Hash collisions on the low-card-side would imply a many-to-many relation.
		if jp.ats == ts {
			return o.newManyToManyMatchErrorOnLowCardSide(jp.sid, histogramID)
		}
		jp.sid = histogramID
		jp.histogramVal = lcs.Histograms[i]
		jp.ats = ts
	}

	sampleHint := len(hcs.Samples) + len(hcs.Histograms)
	histogramHint := len(hcs.Samples) + len(hcs.Histograms)

	for i, histogramID := range hcs.HistogramIDs {
		jp := o.hcJoinBuckets[histogramID]
		if jp.ats != ts {
			continue
		}
		// Hash collisions on the high card side are expected except if a one-to-one
		// matching was requested and we have an implicit many-to-one match instead.
		if jp.bts == ts && o.matching.Card == parser.CardOneToOne {
			return o.newImplicitManyToOneError()
		}
		jp.bts = ts

		var warn warnings.Warnings
		if jp.histogramVal != nil {
			_, h, keep, warn, err = o.computeBinaryPairing(0, 0, hcs.Histograms[i], jp.histogramVal)
		} else {
			_, h, keep, warn, err = o.computeBinaryPairing(0, jp.val, hcs.Histograms[i], nil)
		}
		if err != nil {
			warnings.AddToContext(err, ctx)
			continue
		}
		if warn != 0 {
			emitBinaryOpWarnings(ctx, warn, o.opType)
			// For incompatible types, skip entirely - don't produce any output
			if warn&warnings.WarnIncompatibleTypesInBinOp != 0 {
				continue
			}
		}

		switch {
		case o.returnBool:
			h = nil
			if keep {
				step.AppendSampleWithSizeHint(o.outputSeriesID(histogramID+1, jp.sid+1), 1.0, sampleHint)
			} else {
				step.AppendSampleWithSizeHint(o.outputSeriesID(histogramID+1, jp.sid+1), 0.0, sampleHint)
			}
		case !keep:
			continue
		}

		if h != nil {
			step.AppendHistogramWithSizeHint(o.outputSeriesID(histogramID+1, jp.sid+1), h, histogramHint)
		}
	}

	for i, sampleID := range hcs.SampleIDs {
		jp := o.hcJoinBuckets[sampleID]
		if jp.ats != ts {
			continue
		}
		// Hash collisions on the high card side are expected except if a one-to-one
		// matching was requested and we have an implicit many-to-one match instead.
		if jp.bts == ts && o.matching.Card == parser.CardOneToOne {
			return o.newImplicitManyToOneError()
		}
		jp.bts = ts
		var val float64
		var warn warnings.Warnings

		if jp.histogramVal != nil {
			_, h, keep, warn, err = o.computeBinaryPairing(hcs.Samples[i], 0, nil, jp.histogramVal)
			if err != nil {
				warnings.AddToContext(err, ctx)
				continue
			}
			if warn != 0 {
				emitBinaryOpWarnings(ctx, warn, o.opType)
				if warn&warnings.WarnIncompatibleTypesInBinOp != 0 {
					continue
				}
			}
			if !keep {
				continue
			}
			step.AppendHistogramWithSizeHint(o.outputSeriesID(sampleID+1, jp.sid+1), h, histogramHint)
		} else {
			val, _, keep, warn, err = o.computeBinaryPairing(hcs.Samples[i], jp.val, nil, nil)
			if err != nil {
				warnings.AddToContext(err, ctx)
				continue
			}
			if warn != 0 {
				emitBinaryOpWarnings(ctx, warn, o.opType)
			}
			if o.returnBool {
				val = 0
				if keep {
					val = 1
				}
			} else if !keep {
				continue
			}
			step.AppendSampleWithSizeHint(o.outputSeriesID(sampleID+1, jp.sid+1), val, sampleHint)
		}
	}
	return nil
}

func (o *vectorOperator) newManyToManyMatchErrorOnLowCardSide(originalSampleId, duplicateSampleId uint64) error {
	side := rhBinOpSide
	labels := o.rhsSampleIDs

	if o.matching.Card == parser.CardOneToMany {
		side = lhBinOpSide
		labels = o.lhsSampleIDs
	}
	return newManyToManyMatchError(o.matching, labels[duplicateSampleId], labels[originalSampleId], side)
}

func (o *vectorOperator) newImplicitManyToOneError() error {
	return errors.New("multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)")
}

func (o *vectorOperator) outputSeriesID(hc, lc uint64) uint64 {
	return o.outputMap[cantorPairing(hc, lc)]
}

func (o *vectorOperator) initJoinTables(highCardSide, lowCardSide []labels.Labels) {
	var (
		joinBucketsByHash     = make(map[uint64]*joinBucket)
		lcJoinBuckets         = make([]*joinBucket, len(lowCardSide))
		hcJoinBuckets         = make([]*joinBucket, len(highCardSide))
		lcHashToSeriesIDs     = make(map[uint64][]uint64, len(lowCardSide))
		hcHashToSeriesIDs     = make(map[uint64][]uint64, len(highCardSide))
		lcSampleIdToSignature = make(map[int]uint64, len(lowCardSide))
		hcSampleIdToSignature = make(map[int]uint64, len(highCardSide))

		outputMap = make(map[uint64]uint64, len(highCardSide))
	)

	// initialize join bucket mappings
	for i := range lowCardSide {
		sig := o.sigFunc(lowCardSide[i])
		lcSampleIdToSignature[i] = sig
		lcHashToSeriesIDs[sig] = append(lcHashToSeriesIDs[sig], uint64(i))
		if jb, ok := joinBucketsByHash[sig]; ok {
			lcJoinBuckets[i] = jb
		} else {
			jb := joinBucket{ats: -1, bts: -1}
			joinBucketsByHash[sig] = &jb
			lcJoinBuckets[i] = &jb
		}
	}
	for i := range highCardSide {
		sig := o.sigFunc(highCardSide[i])
		hcSampleIdToSignature[i] = sig
		hcHashToSeriesIDs[sig] = append(hcHashToSeriesIDs[sig], uint64(i))
		if jb, ok := joinBucketsByHash[sig]; ok {
			hcJoinBuckets[i] = jb
		} else {
			jb := joinBucket{ats: -1, bts: -1}
			joinBucketsByHash[sig] = &jb
			hcJoinBuckets[i] = &jb
		}
	}

	// initialize series
	h := &joinHelper{seen: make(map[uint64]int)}
	switch o.opType {
	case parser.LAND:
		// "and" can only have matches if lhs and rhs have collision, so we only need to populate
		// the output map for lhs series that have corresponding hash collision
		for i := range highCardSide {
			sig := hcSampleIdToSignature[i]
			if lcs := lcHashToSeriesIDs[sig]; len(lcs) == 0 {
				continue
			}
			outputMap[cantorPairing(uint64(i+1), 0)] = uint64(h.append(highCardSide[i]))
		}
	case parser.LOR:
		for i := range highCardSide {
			outputMap[cantorPairing(uint64(i+1), 0)] = uint64(h.append(highCardSide[i]))
		}
		for i := range lowCardSide {
			outputMap[cantorPairing(0, uint64(i+1))] = uint64(h.append(lowCardSide[i]))
		}
	case parser.LUNLESS:
		for i := range highCardSide {
			outputMap[cantorPairing(uint64(i+1), 0)] = uint64(h.append(highCardSide[i]))
		}
	default:
		b := labels.NewBuilder(labels.EmptyLabels())
		for i := range highCardSide {
			sig := hcSampleIdToSignature[i]
			lcs, ok := lcHashToSeriesIDs[sig]
			if !ok {
				continue
			}
			for _, lc := range lcs {
				n := h.append(o.resultMetric(b, highCardSide[i], lowCardSide[lc]))
				outputMap[cantorPairing(uint64(i+1), uint64(lc+1))] = uint64(n)
			}
		}
	}
	o.series = h.ls
	o.outputMap = outputMap
	o.lcJoinBuckets = lcJoinBuckets
	o.hcJoinBuckets = hcJoinBuckets
}

type joinHelper struct {
	seen map[uint64]int
	ls   []labels.Labels
	n    int
}

func cantorPairing(hc, lc uint64) uint64 {
	return (hc+lc)*(hc+lc+1)/2 + lc
}

func (h *joinHelper) append(ls labels.Labels) int {
	hash := ls.Hash()
	if n, ok := h.seen[hash]; ok {
		return n
	}
	h.ls = append(h.ls, ls)
	h.seen[hash] = h.n
	h.n++

	return h.n - 1
}

func (o *vectorOperator) resultMetric(b *labels.Builder, highCard, lowCard labels.Labels) labels.Labels {
	b.Reset(highCard)

	if shouldDropMetricName(o.opType, o.returnBool) {
		b.Del(labels.MetricName)
		b.Del(extlabels.MetricType)
		b.Del(extlabels.MetricUnit)
	}

	if o.matching.Card == parser.CardOneToOne {
		if o.matching.On {
			b.Keep(o.matching.MatchingLabels...)
		} else {
			b.Del(o.matching.MatchingLabels...)
		}
	}
	for _, ln := range o.matching.Include {
		if v := lowCard.Get(ln); v != "" {
			b.Set(ln, v)
		} else {
			b.Del(ln)
		}
	}
	if o.returnBool {
		b.Del(labels.MetricName)
		b.Del(extlabels.MetricType)
		b.Del(extlabels.MetricUnit)
	}
	return b.Labels()
}

func signatureFunc(on bool, names ...string) func(labels.Labels) uint64 {
	b := make([]byte, 256)
	if on {
		slices.Sort(names)
		return func(lset labels.Labels) uint64 {
			return xxhash.Sum64(lset.BytesWithLabels(b, names...))
		}
	}
	names = append([]string{labels.MetricName}, names...)
	slices.Sort(names)
	return func(lset labels.Labels) uint64 {
		return xxhash.Sum64(lset.BytesWithoutLabels(b, names...))
	}
}
