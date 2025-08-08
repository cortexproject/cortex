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

const bitsPerWord = 64

type Bitmap struct {
	size int
	bits []uint64
}

// NewBitmap initializes a bitmap
func NewBitmap(size int) *Bitmap {
	return &Bitmap{
		size: size,
		bits: make([]uint64, (size+bitsPerWord-1)/bitsPerWord),
	}
}

// Set sets the bit at position i to 1
func (bm *Bitmap) Set(i int) {
	if i < 0 || i >= bm.size {
		return
	}
	bm.bits[i/bitsPerWord] |= 1 << (i % bitsPerWord)
}

// Clear sets the bit at position i to 0
func (bm *Bitmap) Clear(i int) {
	if i < 0 || i >= bm.size {
		return
	}
	bm.bits[i/bitsPerWord] &^= 1 << (i % bitsPerWord)
}

// Get returns true if the bit at position i is set
func (bm *Bitmap) Get(i int) bool {
	if i < 0 || i >= bm.size {
		return false
	}
	return (bm.bits[i/bitsPerWord] & (1 << (i % bitsPerWord))) != 0
}
