// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/efficientgo/core/errors"
	"github.com/zhangyunhao116/umap"
	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/query"
)

type joinBucket struct {
	ats, bts int64
	sid      uint64
	val      float64
}

// vectorOperator evaluates an expression between two step vectors.
type vectorOperator struct {
	pool *model.VectorPool
	once sync.Once

	lhs          model.VectorOperator
	rhs          model.VectorOperator
	lhsSampleIDs []labels.Labels
	rhsSampleIDs []labels.Labels
	series       []labels.Labels

	// join signature
	sigFunc func(labels.Labels) uint64

	// join helpers
	lcJoinBuckets []*joinBucket
	hcJoinBuckets []*joinBucket

	outputMap *umap.Uint64Map

	matching *parser.VectorMatching
	opType   parser.ItemType

	// If true then 1/0 needs to be returned instead of the value.
	returnBool bool

	model.OperatorTelemetry
}

func NewVectorOperator(
	pool *model.VectorPool,
	lhs model.VectorOperator,
	rhs model.VectorOperator,
	matching *parser.VectorMatching,
	opType parser.ItemType,
	returnBool bool,
	opts *query.Options,
) (model.VectorOperator, error) {
	return &vectorOperator{
		OperatorTelemetry: model.NewTelemetry("[vectorBinary]", opts.EnableAnalysis),

		pool:       pool,
		lhs:        lhs,
		rhs:        rhs,
		matching:   matching,
		opType:     opType,
		returnBool: returnBool,
		sigFunc:    signatureFunc(matching.On, matching.MatchingLabels...),
	}, nil
}

func (o *vectorOperator) Explain() (me string, next []model.VectorOperator) {
	if o.matching.On {
		return fmt.Sprintf("[vectorBinary] %s - %v, on: %v, group: %v", parser.ItemTypeStr[o.opType], o.matching.Card.String(), o.matching.MatchingLabels, o.matching.Include), []model.VectorOperator{o.lhs, o.rhs}
	}
	return fmt.Sprintf("[vectorBinary] %s - %v, ignoring: %v, group: %v", parser.ItemTypeStr[o.opType], o.matching.Card.String(), o.matching.On, o.matching.Include), []model.VectorOperator{o.lhs, o.rhs}
}

func (o *vectorOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	if err := o.initOnce(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *vectorOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Some operators do not call Series of all their children.
	if err := o.initOnce(ctx); err != nil {
		return nil, err
	}

	var lhs []model.StepVector
	var lerrChan = make(chan error, 1)
	go func() {
		var err error
		lhs, err = o.lhs.Next(ctx)
		if err != nil {
			lerrChan <- err
		}
		close(lerrChan)
	}()

	rhs, rerr := o.rhs.Next(ctx)
	lerr := <-lerrChan
	if rerr != nil {
		return nil, rerr
	}
	if lerr != nil {
		return nil, lerr
	}

	// TODO(fpetkovski): When one operator becomes empty,
	// we might want to drain or close the other one.
	// We don't have a concept of closing an operator yet.
	if len(lhs) == 0 || len(rhs) == 0 {
		return nil, nil
	}

	batch := o.pool.GetVectorBatch()
	for i, vector := range lhs {
		if i < len(rhs) {
			step, err := o.execBinaryOperation(lhs[i], rhs[i])
			if err != nil {
				return nil, err
			}
			batch = append(batch, step)
			o.rhs.GetPool().PutStepVector(rhs[i])
		}
		o.lhs.GetPool().PutStepVector(vector)
	}
	o.lhs.GetPool().PutVectors(lhs)
	o.rhs.GetPool().PutVectors(rhs)

	return batch, nil
}

func (o *vectorOperator) GetPool() *model.VectorPool {
	return o.pool
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

	return nil
}

func (o *vectorOperator) execBinaryOperation(lhs, rhs model.StepVector) (model.StepVector, error) {
	switch o.opType {
	case parser.LAND:
		return o.execBinaryAnd(lhs, rhs)
	case parser.LOR:
		return o.execBinaryOr(lhs, rhs)
	case parser.LUNLESS:
		return o.execBinaryUnless(lhs, rhs)
	default:
		return o.execBinaryArithmetic(lhs, rhs)
	}
}

func (o *vectorOperator) execBinaryAnd(lhs, rhs model.StepVector) (model.StepVector, error) {
	ts := lhs.T
	step := o.pool.GetStepVector(ts)

	for _, sampleID := range rhs.SampleIDs {
		jp := o.lcJoinBuckets[sampleID]
		jp.sid = sampleID
		jp.ats = ts
	}
	for i, sampleID := range lhs.SampleIDs {
		if jp := o.hcJoinBuckets[sampleID]; jp.ats == ts {
			step.AppendSample(o.pool, o.outputSeriesID(sampleID+1, jp.sid+1), lhs.Samples[i])
		}
	}
	return step, nil
}

func (o *vectorOperator) execBinaryOr(lhs, rhs model.StepVector) (model.StepVector, error) {
	ts := lhs.T
	step := o.pool.GetStepVector(ts)

	for i, sampleID := range lhs.SampleIDs {
		jp := o.hcJoinBuckets[sampleID]
		jp.ats = ts
		step.AppendSample(o.pool, o.outputSeriesID(sampleID+1, 0), lhs.Samples[i])
	}
	for i, sampleID := range rhs.SampleIDs {
		if jp := o.lcJoinBuckets[sampleID]; jp.ats != ts {
			step.AppendSample(o.pool, o.outputSeriesID(0, sampleID+1), rhs.Samples[i])
		}
	}
	return step, nil
}

func (o *vectorOperator) execBinaryUnless(lhs, rhs model.StepVector) (model.StepVector, error) {
	ts := lhs.T
	step := o.pool.GetStepVector(ts)

	for _, sampleID := range rhs.SampleIDs {
		jp := o.lcJoinBuckets[sampleID]
		jp.ats = ts
	}
	for i, sampleID := range lhs.SampleIDs {
		if jp := o.hcJoinBuckets[sampleID]; jp.ats != ts {
			step.AppendSample(o.pool, o.outputSeriesID(sampleID+1, 0), lhs.Samples[i])
		}
	}
	return step, nil
}

// TODO: add support for histogram.
func (o *vectorOperator) computeBinaryPairing(hval, lval float64) (float64, bool) {
	// operand is not commutative so we need to address potential swapping
	if o.matching.Card == parser.CardOneToMany {
		v, _, keep := vectorElemBinop(o.opType, lval, hval, nil, nil)
		return v, keep
	}
	v, _, keep := vectorElemBinop(o.opType, hval, lval, nil, nil)
	return v, keep
}

func (o *vectorOperator) execBinaryArithmetic(lhs, rhs model.StepVector) (model.StepVector, error) {
	ts := lhs.T
	step := o.pool.GetStepVector(ts)

	var (
		hcs, lcs model.StepVector
	)

	switch o.matching.Card {
	case parser.CardManyToOne, parser.CardOneToOne:
		hcs, lcs = lhs, rhs
	case parser.CardOneToMany:
		hcs, lcs = rhs, lhs
	default:
		return step, errors.Newf("Unexpected matching cardinality: %s", o.matching.Card.String())
	}

	// shortcut: if we have no samples on the high card side we cannot compute pairings
	if len(hcs.Samples) == 0 {
		return step, nil
	}

	for i, sampleID := range lcs.SampleIDs {
		jp := o.lcJoinBuckets[sampleID]
		// Hash collisions on the low-card-side would imply a many-to-many relation.
		if jp.ats == ts {
			return model.StepVector{}, o.newManyToManyMatchErrorOnLowCardSide(jp.sid, sampleID)
		}
		jp.sid = sampleID
		jp.val = lcs.Samples[i]
		jp.ats = ts
	}

	for i, sampleID := range hcs.SampleIDs {
		jp := o.hcJoinBuckets[sampleID]
		if jp.ats != ts {
			continue
		}
		// Hash collisions on the high card side are expected except if a one-to-one
		// matching was requested and we have an implicit many-to-one match instead.
		if jp.bts == ts && o.matching.Card == parser.CardOneToOne {
			return model.StepVector{}, o.newImplicitManyToOneError()
		}
		jp.bts = ts

		val, keep := o.computeBinaryPairing(hcs.Samples[i], jp.val)
		if o.returnBool {
			val = 0
			if keep {
				val = 1
			}
		} else if !keep {
			continue
		}
		step.AppendSample(o.pool, o.outputSeriesID(sampleID+1, jp.sid+1), val)
	}
	return step, nil
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
	res, _ := o.outputMap.Load(cantorPairing(hc, lc))
	return res
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

		outputMap = umap.New64(len(highCardSide))
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
		for i := range highCardSide {
			sig := hcSampleIdToSignature[i]
			lcs, ok := lcHashToSeriesIDs[sig]
			if !ok {
				continue
			}
			for _, lc := range lcs {
				outputMap.Store(cantorPairing(uint64(i+1), uint64(lc+1)), uint64(h.append(highCardSide[i])))
			}
		}
	case parser.LOR:
		for i := range highCardSide {
			outputMap.Store(cantorPairing(uint64(i+1), 0), uint64(h.append(highCardSide[i])))
		}
		for i := range lowCardSide {
			outputMap.Store(cantorPairing(0, uint64(i+1)), uint64(h.append(lowCardSide[i])))
		}
	case parser.LUNLESS:
		for i := range highCardSide {
			outputMap.Store(cantorPairing(uint64(i+1), 0), uint64(h.append(highCardSide[i])))
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
				outputMap.Store(cantorPairing(uint64(i+1), uint64(lc+1)), uint64(n))
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

// Lifted from: https://github.com/prometheus/prometheus/blob/a38179c4e183d9b50b271167bf90050eda8ec3d1/promql/engine.go#L2430.
// TODO: call with histogram values in followup PR.
// nolint: unparam
func vectorElemBinop(op parser.ItemType, lhs, rhs float64, hlhs, hrhs *histogram.FloatHistogram) (float64, *histogram.FloatHistogram, bool) {
	switch op {
	case parser.ADD:
		if hlhs != nil && hrhs != nil {
			// The histogram being added must have the larger schema
			// code (i.e. the higher resolution).
			if hrhs.Schema >= hlhs.Schema {
				return 0, hlhs.Copy().Add(hrhs).Compact(0), true
			}
			return 0, hrhs.Copy().Add(hlhs).Compact(0), true
		}
		return lhs + rhs, nil, true
	case parser.SUB:
		if hlhs != nil && hrhs != nil {
			// The histogram being subtracted must have the larger schema
			// code (i.e. the higher resolution).
			if hrhs.Schema >= hlhs.Schema {
				return 0, hlhs.Copy().Sub(hrhs).Compact(0), true
			}
			return 0, hrhs.Copy().Mul(-1).Add(hlhs).Compact(0), true
		}
		return lhs - rhs, nil, true
	case parser.MUL:
		if hlhs != nil && hrhs == nil {
			return 0, hlhs.Copy().Mul(rhs), true
		}
		if hlhs == nil && hrhs != nil {
			return 0, hrhs.Copy().Mul(lhs), true
		}
		return lhs * rhs, nil, true
	case parser.DIV:
		if hlhs != nil && hrhs == nil {
			return 0, hlhs.Copy().Div(rhs), true
		}
		return lhs / rhs, nil, true
	case parser.POW:
		return math.Pow(lhs, rhs), nil, true
	case parser.MOD:
		return math.Mod(lhs, rhs), nil, true
	case parser.EQLC:
		return lhs, nil, lhs == rhs
	case parser.NEQ:
		return lhs, nil, lhs != rhs
	case parser.GTR:
		return lhs, nil, lhs > rhs
	case parser.LSS:
		return lhs, nil, lhs < rhs
	case parser.GTE:
		return lhs, nil, lhs >= rhs
	case parser.LTE:
		return lhs, nil, lhs <= rhs
	case parser.ATAN2:
		return math.Atan2(lhs, rhs), nil, true
	}
	panic(errors.Newf("operator %q not allowed for operations between Vectors", op))
}
