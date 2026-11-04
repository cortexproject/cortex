// Modified from github.com/prometheus/common/model/fnv.go
// Copyright 2015 The Prometheus Authors
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

// Inline and byte-free variant of hash/fnv's fnv64a and fnv32.

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
	offset32 = 2166136261
	prime32  = 16777619
)

// HashNew initializes a new fnv64a hash value.
func HashNew() uint64 {
	return offset64
}

// HashAdd adds a string to a fnv64a hash value, returning the updated hash.
// Note this is the same algorithm as Go stdlib `sum64a.Write()`
func HashAdd(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// HashAddByte adds a byte to a fnv64a hash value, returning the updated hash.
func HashAddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= prime64
	return h
}

// HashAddUint adds a uint64 to a fnv64a hash value, returning the updated hash.
func HashAddUint(h uint64, i uint64) uint64 {
	h ^= i
	h *= prime64
	return h
}

// HashNew32 initializes a new fnv32 hash value.
func HashNew32() uint32 {
	return offset32
}

// HashAdd32 adds a string to a fnv32 hash value, returning the updated hash.
// Note this is the same algorithm as Go stdlib `sum32.Write()`
func HashAdd32(h uint32, s string) uint32 {
	for i := 0; i < len(s); i++ {
		h *= prime32
		h ^= uint32(s[i])
	}
	return h
}

// HashAddByte32 adds a byte to a fnv32 hash value, returning the updated hash.
func HashAddByte32(h uint32, b byte) uint32 {
	h *= prime32
	h ^= uint32(b)
	return h
}

// HashAddUint32 adds a uint32 to a fnv32 hash value, returning the updated hash.
func HashAddUint32(h uint32, i uint32) uint32 {
	h *= prime32
	h ^= i
	return h
}
