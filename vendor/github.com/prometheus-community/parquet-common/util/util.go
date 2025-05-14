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

package util

import (
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

func YoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

func CloneRows(rows []parquet.Row) []parquet.Row {
	rr := make([]parquet.Row, len(rows))
	for i, row := range rows {
		rr[i] = row.Clone()
	}
	return rr
}

// Copied from thanos repository:
// https://github.com/thanos-io/thanos/blob/2a5a856e34adb2653dda700c4d87637236afb2dd/pkg/store/bucket.go#L3466

type Part struct {
	Start uint64
	End   uint64

	ElemRng [2]int
}

type Partitioner interface {
	// Partition partitions length entries into n <= length ranges that cover all
	// input ranges
	// It supports overlapping ranges.
	// NOTE: It expects range to be sorted by start time.
	Partition(length int, rng func(int) (uint64, uint64)) []Part
}

type gapBasedPartitioner struct {
	maxGapSize uint64
}

func NewGapBasedPartitioner(maxGapSize uint64) Partitioner {
	return gapBasedPartitioner{
		maxGapSize: maxGapSize,
	}
}

// Partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
// It is used to combine multiple small ranges from object storage into bigger, more efficient/cheaper ones.
func (g gapBasedPartitioner) Partition(length int, rng func(int) (uint64, uint64)) (parts []Part) {
	j := 0
	k := 0
	for k < length {
		j = k
		k++

		p := Part{}
		p.Start, p.End = rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if p.End+g.maxGapSize < s {
				break
			}

			if p.End <= e {
				p.End = e
			}
		}
		p.ElemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return parts
}
