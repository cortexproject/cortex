// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"context"
	"fmt"
	"sync"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"

	"github.com/thanos-community/promql-engine/execution/model"
)

// vectorOperator evaluates an expression between two step vectors.
type vectorOperator struct {
	pool *model.VectorPool
	once sync.Once

	lhs            model.VectorOperator
	rhs            model.VectorOperator
	matching       *parser.VectorMatching
	groupingLabels []string
	operation      operation
	opType         parser.ItemType

	lhSampleIDs []labels.Labels
	rhSampleIDs []labels.Labels

	// series contains the output series of the operator
	series []labels.Labels
	// The outputCache is an internal cache used to calculate
	// the binary operation of the lhs and rhs operator.
	outputCache []outputSample
	// table is used to calculate the binary operation of two step vectors between
	// the lhs and rhs operator.
	table *table

	// If true then 1/0 needs to be returned instead of the value.
	returnBool bool
}

func NewVectorOperator(
	pool *model.VectorPool,
	lhs model.VectorOperator,
	rhs model.VectorOperator,
	matching *parser.VectorMatching,
	operation parser.ItemType,
	returnBool bool,
) (model.VectorOperator, error) {
	op, err := newOperation(operation, true)
	if err != nil {
		return nil, err
	}

	// Make a copy of MatchingLabels to avoid potential side-effects
	// in some downstream operation.
	groupings := make([]string, len(matching.MatchingLabels))
	copy(groupings, matching.MatchingLabels)
	slices.Sort(groupings)

	return &vectorOperator{
		pool:           pool,
		lhs:            lhs,
		rhs:            rhs,
		matching:       matching,
		groupingLabels: groupings,
		operation:      op,
		opType:         operation,
		returnBool:     returnBool,
	}, nil
}

func (o *vectorOperator) Explain() (me string, next []model.VectorOperator) {
	if o.matching.On {
		return fmt.Sprintf("[*vectorOperator] %s %v on %v group %v", parser.ItemTypeStr[o.opType], o.matching.Card.String(), o.matching.MatchingLabels, o.matching.Include), []model.VectorOperator{o.lhs, o.rhs}
	}
	return fmt.Sprintf("[*vectorOperator] %s %v ignoring %v group %v", parser.ItemTypeStr[o.opType], o.matching.Card.String(), o.matching.On, o.matching.Include), []model.VectorOperator{o.lhs, o.rhs}
}

func (o *vectorOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	o.once.Do(func() { err = o.initOutputs(ctx) })
	if err != nil {
		return nil, err
	}

	return o.series, nil
}

func (o *vectorOperator) initOutputs(ctx context.Context) error {
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

	o.lhSampleIDs = highCardSide
	o.rhSampleIDs = lowCardSide

	if o.matching.Card == parser.CardOneToMany {
		highCardSide, lowCardSide = lowCardSide, highCardSide
	}

	buf := make([]byte, 1024)
	var includeLabels []string
	if len(o.matching.Include) > 0 {
		includeLabels = o.matching.Include
	}
	keepLabels := o.matching.Card != parser.CardOneToOne
	keepName := o.opType.IsComparisonOperator()
	highCardHashes, highCardInputMap := o.hashSeries(highCardSide, keepLabels, keepName, buf)
	lowCardHashes, lowCardInputMap := o.hashSeries(lowCardSide, keepLabels, keepName, buf)
	output, highCardOutputIndex, lowCardOutputIndex := o.join(highCardHashes, highCardInputMap, lowCardHashes, lowCardInputMap, includeLabels)

	series := make([]labels.Labels, len(output))
	for _, s := range output {
		series[s.ID] = s.Metric
	}
	o.series = series

	o.outputCache = make([]outputSample, len(series))
	for i := range o.outputCache {
		o.outputCache[i].lhT = -1
	}
	o.pool.SetStepSize(len(highCardSide))

	o.table = newTable(
		o.pool,
		o.matching.Card,
		o.operation,
		o.outputCache,
		newHighCardIndex(highCardOutputIndex),
		lowCardinalityIndex(lowCardOutputIndex),
	)

	return nil
}

func (o *vectorOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	lhs, err := o.lhs.Next(ctx)
	if err != nil {
		return nil, err
	}
	rhs, err := o.rhs.Next(ctx)
	if err != nil {
		return nil, err
	}

	// TODO(fpetkovski): When one operator becomes empty,
	// we might want to drain or close the other one.
	// We don't have a concept of closing an operator yet.
	if len(lhs) == 0 || len(rhs) == 0 {
		return nil, nil
	}

	o.once.Do(func() { err = o.initOutputs(ctx) })
	if err != nil {
		return nil, err
	}

	batch := o.pool.GetVectorBatch()
	for i, vector := range lhs {
		if i < len(rhs) {
			step, err := o.table.execBinaryOperation(lhs[i], rhs[i], o.returnBool)
			if err == nil {
				batch = append(batch, step)
				o.rhs.GetPool().PutStepVector(rhs[i])
				continue
			}

			var sampleID, duplicateSampleID labels.Labels
			switch err.side {
			case lhBinOpSide:
				sampleID = o.lhSampleIDs[err.sampleID]
				duplicateSampleID = o.lhSampleIDs[err.duplicateSampleID]
			case rhBinOpSide:
				sampleID = o.rhSampleIDs[err.sampleID]
				duplicateSampleID = o.rhSampleIDs[err.duplicateSampleID]
			}
			group := sampleID.MatchLabels(o.matching.On, o.matching.MatchingLabels...)
			msg := "found duplicate series for the match group %s on the %s hand-side of the operation: [%s, %s]" +
				";many-to-many matching not allowed: matching labels must be unique on one side"
			return nil, errors.Newf(msg, group, err.side, sampleID.String(), duplicateSampleID.String())
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

// hashSeries calculates the hash of each series from an input operator.
// Since series from the high cardinality operator can map to multiple output series,
// hashSeries returns an index from hash to a slice of resulting series, and
// a map from input series ID to output series ID.
// The latter can be used to build an array backed index from input model.Series to output model.Series,
// avoiding expensive hashmap lookups.
func (o *vectorOperator) hashSeries(series []labels.Labels, keepLabels, keepName bool, buf []byte) (map[uint64][]model.Series, map[uint64][]uint64) {
	hashes := make(map[uint64][]model.Series)
	inputIndex := make(map[uint64][]uint64)
	for i, s := range series {
		sig, lbls := signature(s, !o.matching.On, o.groupingLabels, keepLabels, keepName, buf)
		if _, ok := hashes[sig]; !ok {
			hashes[sig] = make([]model.Series, 0, 1)
			inputIndex[sig] = make([]uint64, 0, 1)
		}
		hashes[sig] = append(hashes[sig], model.Series{
			ID:     uint64(i),
			Metric: lbls,
		})
		inputIndex[sig] = append(inputIndex[sig], uint64(i))
	}

	return hashes, inputIndex
}

// join performs a join between series from the high cardinality and low cardinality operators.
// It does that by using hash maps which point from series hash to the output series.
// It also returns array backed indices for the high cardinality and low cardinality operators,
// pointing from input model.Series ID to output model.Series ID.
// The high cardinality operator can fail to join, which is why its index contains nullable values.
// The low cardinality operator can join to multiple high cardinality series, which is why its index
// points to an array of output series.
func (o *vectorOperator) join(
	highCardHashes map[uint64][]model.Series,
	highCardInputIndex map[uint64][]uint64,
	lowCardHashes map[uint64][]model.Series,
	lowCardInputIndex map[uint64][]uint64,
	includeLabels []string,
) ([]model.Series, []*uint64, [][]uint64) {
	// Output index points from output series ID
	// to the actual series.
	outputIndex := make([]model.Series, 0)

	// Prune high cardinality series which do not have a
	// matching low cardinality series.
	outputSize := 0
	for hash, series := range highCardHashes {
		outputSize += len(series)
		if _, ok := lowCardHashes[hash]; !ok {
			delete(highCardHashes, hash)
			continue
		}
	}
	lowCardOutputSize := 0
	for _, lowCardOutputs := range lowCardInputIndex {
		lowCardOutputSize += len(lowCardOutputs)
	}

	highCardOutputIndex := make([]*uint64, outputSize)
	lowCardOutputIndex := make([][]uint64, lowCardOutputSize)
	for hash, highCardSeries := range highCardHashes {
		for _, lowCardSeriesID := range lowCardInputIndex[hash] {
			// Each low cardinality series can map to multiple output series.
			lowCardOutputIndex[lowCardSeriesID] = make([]uint64, 0, len(highCardSeries))
		}

		lowCardSeries := lowCardHashes[hash][0]
		for i, output := range highCardSeries {
			outputSeries := buildOutputSeries(uint64(len(outputIndex)), output, lowCardSeries, includeLabels)
			outputIndex = append(outputIndex, outputSeries)

			highCardSeriesID := highCardInputIndex[hash][i]
			highCardOutputIndex[highCardSeriesID] = &outputSeries.ID

			for _, lowCardSeriesID := range lowCardInputIndex[hash] {
				lowCardOutputIndex[lowCardSeriesID] = append(lowCardOutputIndex[lowCardSeriesID], outputSeries.ID)
			}
		}
	}

	return outputIndex, highCardOutputIndex, lowCardOutputIndex
}

func signature(metric labels.Labels, without bool, grouping []string, keepOriginalLabels, keepName bool, buf []byte) (uint64, labels.Labels) {
	buf = buf[:0]
	lb := labels.NewBuilder(metric)
	if !keepName {
		lb = lb.Del(labels.MetricName)
	}
	if without {
		dropLabels := grouping
		if !keepName {
			dropLabels = append(grouping, labels.MetricName)
		}
		key, _ := metric.HashWithoutLabels(buf, dropLabels...)
		if !keepOriginalLabels {
			lb.Del(dropLabels...)
		}
		return key, lb.Labels(nil)
	}

	if !keepOriginalLabels {
		lb.Keep(grouping...)
	}
	if len(grouping) == 0 {
		return 0, lb.Labels(nil)
	}

	key, _ := metric.HashForLabels(buf, grouping...)
	return key, lb.Labels(nil)
}

func buildOutputSeries(seriesID uint64, highCardSeries, lowCardSeries model.Series, includeLabels []string) model.Series {
	metric := highCardSeries.Metric
	if len(includeLabels) > 0 {
		lowCardLabels := labels.NewBuilder(lowCardSeries.Metric).
			Keep(includeLabels...).
			Labels(nil)
		metric = append(metric, lowCardLabels...)
	}
	return model.Series{ID: seriesID, Metric: metric}
}
