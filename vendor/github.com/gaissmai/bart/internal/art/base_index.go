// Copyright (c) 2024 Karl Gaissmaier
// SPDX-License-Identifier: MIT

// Package art summarizes the functions and inverse functions
// for mapping between a prefix and a baseIndex.
//
//	can inline HostIdx with cost 5
//	can inline IdxToPfx256 with cost 37
//	can inline IdxToRange256 with cost 61
//	can inline NetMask with cost 7
//	can inline PfxLen256 with cost 18
//	can inline PfxToIdx256 with cost 29
//	can inline pfxToIdx with cost 11
//
// Please read the ART paper ./doc/artlookup.pdf
// to understand the baseIndex algorithm.
package art

import "math/bits"

// PfxToIdx maps 8bit prefixes to numbers. The prefixes range from 0/0 to 255/7
// The return values range from 1 to 255.
//
//	  [0x0000_00001 .. 0x1111_1111] = [1 .. 255]
//
//		example: octet/pfxLen: 160/3 = 0b1010_0000/3 => IdxToPfx(160/3) => 13
//
//		                0b1010_0000 => 0b0000_0101
//		                  ^^^ >> (8-3)         ^^^
//
//		                0b0000_0001 => 0b0000_1000
//		                          ^ << 3      ^
//		                 + -----------------------
//		                               0b0000_1101 = 13
func PfxToIdx(octet, pfxLen uint8) uint8 {
	return octet>>(8-pfxLen) + 1<<pfxLen
}

// OctetToIdx maps octet/8 prefixes to numbers. The return values range from 256 to 511.
// OctetToIdx is a special case of PfxToIdx.
func OctetToIdx(octet uint8) uint {
	return uint(octet) + 256
}

// IdxToPfx returns the octet and prefix len of baseIdx.
// It's the inverse to pfxToIdx256.
func IdxToPfx(idx uint8) (octet, pfxLen uint8) {
	pfxLen = uint8(bits.Len8(idx)) - 1
	shiftBits := 8 - pfxLen

	mask := uint8(0xff) >> shiftBits
	octet = (idx & mask) << shiftBits

	return
}

// PfxBits returns the bits based on depth and idx.
func PfxBits(depth int, idx uint8) uint8 {
	return uint8(depth<<3 + bits.Len8(idx) - 1)
}

// IdxToRange returns the first and last octet of prefix idx.
func IdxToRange(idx uint8) (first, last uint8) {
	first, pfxLen := IdxToPfx(idx)
	last = first | ^NetMask(pfxLen)
	return
}

// NetMask for bits
//
//	0b0000_0000, // bits == 0
//	0b1000_0000, // bits == 1
//	0b1100_0000, // bits == 2
//	0b1110_0000, // bits == 3
//	0b1111_0000, // bits == 4
//	0b1111_1000, // bits == 5
//	0b1111_1100, // bits == 6
//	0b1111_1110, // bits == 7
//	0b1111_1111, // bits == 8
func NetMask(bits uint8) uint8 {
	return 0b1111_1111 << (8 - uint16(bits))
}
