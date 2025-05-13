// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package convert

import (
	"container/heap"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

func NewMergeChunkSeriesSet(sets []storage.ChunkSeriesSet, compare func(a, b labels.Labels) int, mergeFunc storage.VerticalChunkSeriesMergeFunc) storage.ChunkSeriesSet {
	h := heapChunkSeries{
		heap:    make([]storage.ChunkSeriesSet, 0, len(sets)),
		compare: compare,
	}
	for _, set := range sets {
		if set == nil {
			continue
		}
		if set.Next() {
			heap.Push(&h, set)
		}
		if err := set.Err(); err != nil {
			return &mergeChunkSeriesSet{err: err}
		}
	}

	return &mergeChunkSeriesSet{
		h:         h,
		mergeFunc: mergeFunc,
	}
}

type mergeChunkSeriesSet struct {
	h           heapChunkSeries
	mergeFunc   storage.VerticalChunkSeriesMergeFunc
	currentSets []storage.ChunkSeriesSet

	err error
}

func (m *mergeChunkSeriesSet) Next() bool {
	for _, set := range m.currentSets {
		if set == nil {
			continue
		}
		if set.Next() {
			heap.Push(&m.h, set)
		}
		if err := set.Err(); err != nil {
			m.err = err
			break
		}
	}

	if len(m.h.heap) == 0 {
		return false
	}

	m.currentSets = m.currentSets[:0]
	currentLabels := m.h.heap[0].At().Labels()

	for len(m.h.heap) > 0 && labels.Equal(m.h.heap[0].At().Labels(), currentLabels) {
		m.currentSets = append(m.currentSets, heap.Pop(&m.h).(storage.ChunkSeriesSet))
	}

	return len(m.currentSets) > 0
}

func (m *mergeChunkSeriesSet) At() storage.ChunkSeries {
	if len(m.currentSets) == 1 {
		return m.currentSets[0].At()
	}
	series := make([]storage.ChunkSeries, 0, len(m.currentSets))
	for _, seriesSet := range m.currentSets {
		series = append(series, seriesSet.At())
	}
	return m.mergeFunc(series...)
}

func (m *mergeChunkSeriesSet) Err() error {
	return m.err
}

func (m *mergeChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

type heapChunkSeries struct {
	heap    []storage.ChunkSeriesSet
	compare func(a, b labels.Labels) int
}

func (h *heapChunkSeries) Len() int { return len(h.heap) }

func (h *heapChunkSeries) Less(i, j int) bool {
	a, b := h.heap[i].At().Labels(), h.heap[j].At().Labels()
	return h.compare(a, b) < 0
}

func (h *heapChunkSeries) Swap(i, j int) { h.heap[i], h.heap[j] = h.heap[j], h.heap[i] }

func (h *heapChunkSeries) Push(x any) {
	h.heap = append(h.heap, x.(storage.ChunkSeriesSet))
}

func (h *heapChunkSeries) Pop() any {
	old := h.heap
	n := len(old)
	x := old[n-1]
	h.heap = old[0 : n-1]
	return x
}
