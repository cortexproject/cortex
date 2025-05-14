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

package search

import (
	"sort"
)

type RowRange struct {
	from  int64
	count int64
}

func NewRowRange(from, count int64) *RowRange {
	return &RowRange{
		from:  from,
		count: count,
	}
}

// Overlaps returns true if the receiver and the given RowRange share any overlapping rows.
// Both ranges are treated as half-open intervals: [from, from+count).
func (rr RowRange) Overlaps(o RowRange) bool {
	endA := rr.from + rr.count
	endB := o.from + o.count
	return rr.from < endB && o.from < endA
}

// intersect intersects the row ranges from left hand sight with the row ranges from rhs
// it assumes that lhs and rhs are simplified and returns a simplified result.
// it operates in o(l+r) time by cursoring through ranges with a two pointer approach.
func intersectRowRanges(lhs, rhs []RowRange) []RowRange {
	res := make([]RowRange, 0)
	for l, r := 0, 0; l < len(lhs) && r < len(rhs); {
		al, bl := lhs[l].from, lhs[l].from+lhs[l].count
		ar, br := rhs[r].from, rhs[r].from+rhs[r].count

		// check if rows intersect
		if al <= br && ar <= bl {
			os, oe := max(al, ar), min(bl, br)
			res = append(res, RowRange{from: os, count: oe - os})
		}

		// advance the cursor of the range that ends first
		if bl <= br {
			l++
		} else {
			r++
		}
	}
	return simplify(res)
}

// complementRowRanges returns the ranges that are in rhs but not in lhs.
// For example, if you have:
// lhs: [{from: 1, count: 3}]  // represents rows 1,2,3
// rhs: [{from: 0, count: 5}]  // represents rows 0,1,2,3,4
// The complement would be [{from: 0, count: 1}, {from: 4, count: 1}]  // represents rows 0,4
// because these are the rows in rhs that are not in lhs.
//
// The function assumes that lhs and rhs are simplified (no overlapping ranges)
// and returns a simplified result. It operates in O(l+r) time by using a two-pointer approach
// to efficiently process both ranges.
func complementRowRanges(lhs, rhs []RowRange) []RowRange {
	res := make([]RowRange, 0)

	l, r := 0, 0
	for l < len(lhs) && r < len(rhs) {
		al, bl := lhs[l].from, lhs[l].from+lhs[l].count
		ar, br := rhs[r].from, rhs[r].from+rhs[r].count

		// check if rows intersect
		switch {
		case al > br || ar > bl:
			// no intersection, advance cursor that ends first
			if bl <= br {
				l++
			} else {
				res = append(res, RowRange{from: ar, count: br - ar})
				r++
			}
		case al < ar && bl > br:
			// l contains r, complement of l in r is empty, advance r
			r++
		case al < ar && bl <= br:
			// l covers r from left but has room on top
			oe := min(bl, br)
			rhs[r].from += oe - ar
			rhs[r].count -= oe - ar
			l++
		case al >= ar && bl > br:
			// l covers r from right but has room on bottom
			os := max(al, ar)
			res = append(res, RowRange{from: ar, count: os - ar})
			r++
		case al >= ar && bl <= br:
			// l is included r
			os, oe := max(al, ar), min(bl, br)
			res = append(res, RowRange{from: rhs[r].from, count: os - rhs[r].from})
			rhs[r].from = oe
			rhs[r].count = br - oe
			l++
		}
	}

	for ; r < len(rhs); r++ {
		res = append(res, rhs[r])
	}

	return simplify(res)
}

func simplify(rr []RowRange) []RowRange {
	if len(rr) == 0 {
		return nil
	}

	sort.Slice(rr, func(i, j int) bool {
		return rr[i].from < rr[j].from
	})

	tmp := make([]RowRange, 0)
	l := rr[0]
	for i := 1; i < len(rr); i++ {
		r := rr[i]
		al, bl := l.from, l.from+l.count
		ar, br := r.from, r.from+r.count
		if bl < ar {
			tmp = append(tmp, l)
			l = r
			continue
		}

		from := min(al, ar)
		count := max(bl, br) - from
		if count == 0 {
			continue
		}

		l = RowRange{
			from:  from,
			count: count,
		}
	}

	tmp = append(tmp, l)
	res := make([]RowRange, 0, len(tmp))
	for i := range tmp {
		if tmp[i].count != 0 {
			res = append(res, tmp[i])
		}
	}

	return res
}
