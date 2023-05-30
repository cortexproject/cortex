// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

type outputIndex interface {
	outputSamples(inputSampleID uint64) []uint64
}

type highCardinalityIndex struct {
	result []uint64
	index  []*uint64
}

func newHighCardIndex(index []*uint64) *highCardinalityIndex {
	return &highCardinalityIndex{
		result: make([]uint64, 1),
		index:  index,
	}
}

func (h *highCardinalityIndex) outputSamples(inputSampleID uint64) []uint64 {
	outputSampleID := h.index[inputSampleID]
	if outputSampleID == nil {
		return nil
	}
	h.result[0] = *outputSampleID
	return h.result
}

type lowCardinalityIndex [][]uint64

func (l lowCardinalityIndex) outputSamples(inputSampleID uint64) []uint64 {
	return l[inputSampleID]
}
