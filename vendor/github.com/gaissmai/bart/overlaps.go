// Copyright (c) 2024 Karl Gaissmaier
// SPDX-License-Identifier: MIT

package bart

import (
	"net/netip"

	"github.com/gaissmai/bart/internal/allot"
	"github.com/gaissmai/bart/internal/art"
	"github.com/gaissmai/bart/internal/bitset"
)

// overlaps returns true if any IP in the nodes n or o overlaps.
func (n *node[V]) overlaps(o *node[V], depth int) bool {
	nPfxCount := n.prefixes.Len()
	oPfxCount := o.prefixes.Len()

	nChildCount := n.children.Len()
	oChildCount := o.children.Len()

	// ##############################
	// 1. Test if any routes overlaps
	// ##############################

	// full cross check
	if nPfxCount > 0 && oPfxCount > 0 {
		if n.overlapsRoutes(o) {
			return true
		}
	}

	// ####################################
	// 2. Test if routes overlaps any child
	// ####################################

	// swap nodes to help chance on its way,
	// if the first call to expensive overlapsChildrenIn() is already true,
	// if both orders are false it doesn't help either
	if nChildCount > oChildCount {
		n, o = o, n

		nPfxCount = n.prefixes.Len()
		oPfxCount = o.prefixes.Len()

		nChildCount = n.children.Len()
		oChildCount = o.children.Len()
	}

	if nPfxCount > 0 && oChildCount > 0 {
		if n.overlapsChildrenIn(o) {
			return true
		}
	}

	// symmetric reverse
	if oPfxCount > 0 && nChildCount > 0 {
		if o.overlapsChildrenIn(n) {
			return true
		}
	}

	// ###########################################
	// 3. childs with same octet in nodes n and o
	// ###########################################

	// stop condition, n or o have no childs
	if nChildCount == 0 || oChildCount == 0 {
		return false
	}

	// stop condition, no child with identical octet in n and o
	if !n.children.Intersects(&o.children.BitSet256) {
		return false
	}

	return n.overlapsSameChildren(o, depth)
}

// overlapsRoutes, test if n overlaps o prefixes and vice versa
func (n *node[V]) overlapsRoutes(o *node[V]) bool {
	// some prefixes are identical, trivial overlap
	if n.prefixes.Intersects(&o.prefixes.BitSet256) {
		return true
	}

	// get the lowest idx (biggest prefix)
	nFirstIdx, _ := n.prefixes.FirstSet()
	oFirstIdx, _ := o.prefixes.FirstSet()

	// start with other min value
	nIdx := oFirstIdx
	oIdx := nFirstIdx

	nOK := true
	oOK := true

	// zip, range over n and o together to help chance on its way
	for nOK || oOK {
		if nOK {
			// does any route in o overlap this prefix from n
			if nIdx, nOK = n.prefixes.NextSet(nIdx); nOK {
				if o.lpmTest(uint(nIdx)) {
					return true
				}

				if nIdx == 255 {
					// stop, don't overflow uint8!
					nOK = false
				} else {
					nIdx++
				}
			}
		}

		if oOK {
			// does any route in n overlap this prefix from o
			if oIdx, oOK = o.prefixes.NextSet(oIdx); oOK {
				if n.lpmTest(uint(oIdx)) {
					return true
				}

				if oIdx == 255 {
					// stop, don't overflow uint8!
					oOK = false
				} else {
					oIdx++
				}
			}
		}
	}

	return false
}

// overlapsChildrenIn, test if prefixes in n overlaps child octets in o.
func (n *node[V]) overlapsChildrenIn(o *node[V]) bool {
	pfxCount := n.prefixes.Len()
	childCount := o.children.Len()

	// heuristic, compare benchmarks
	// when will we range over the children and when will we do bitset calc?
	magicNumber := 15
	doRange := childCount < magicNumber || pfxCount > magicNumber

	// do range over, not so many childs and maybe too many prefixes for other algo below
	if doRange {
		for _, addr := range o.children.AsSlice(&[256]uint8{}) {
			if n.lpmTest(art.OctetToIdx(addr)) {
				return true
			}
		}
		return false
	}

	// do bitset intersection, alloted route table with child octets
	// maybe too many childs for range-over or not so many prefixes to
	// build the alloted routing table from them

	// make allot table with prefixes as bitsets, bitsets are precalculated.
	// Just union the bitsets to one bitset (allot table) for all prefixes
	// in this node
	hostRoutes := bitset.BitSet256{}

	allIndices := n.prefixes.AsSlice(&[256]uint8{})

	// union all pre alloted bitsets
	for _, idx := range allIndices {
		hostRoutes = hostRoutes.Union(allot.IdxToFringeRoutes(idx))
	}

	return hostRoutes.Intersects(&o.children.BitSet256)
}

// overlapsSameChildren, find same octets with bitset intersection.
func (n *node[V]) overlapsSameChildren(o *node[V], depth int) bool {
	// intersect the child bitsets from n with o
	commonChildren := n.children.Intersection(&o.children.BitSet256)

	addr := uint8(0)
	ok := true
	for ok {
		if addr, ok = commonChildren.NextSet(addr); ok {
			nChild := n.children.MustGet(addr)
			oChild := o.children.MustGet(addr)

			if overlapsTwoChilds[V](nChild, oChild, depth+1) {
				return true
			}

			if addr == 255 {
				// stop, don't overflow uint8!
				ok = false
			} else {
				addr++
			}
		}
	}
	return false
}

// overlapsTwoChilds, childs can be node or leaf.
func overlapsTwoChilds[V any](nChild, oChild any, depth int) bool {
	//  3x3 possible different combinations for n and o
	//
	//  node, node    --> overlaps rec descent
	//  node, leaf    --> overlapsPrefixAtDepth
	//  node, fringe  --> true
	//
	//  leaf, node    --> overlapsPrefixAtDepth
	//  leaf, leaf    --> netip.Prefix.Overlaps
	//  leaf, fringe  --> true
	//
	//  fringe, node    --> true
	//  fringe, leaf    --> true
	//  fringe, fringe  --> true
	//
	switch nKind := nChild.(type) {
	case *node[V]: // node, ...
		switch oKind := oChild.(type) {
		case *node[V]: // node, node
			return nKind.overlaps(oKind, depth)
		case *leafNode[V]: // node, leaf
			return nKind.overlapsPrefixAtDepth(oKind.prefix, depth)
		case *fringeNode[V]: // node, fringe
			return true
		default:
			panic("logic error, wrong node type")
		}

	case *leafNode[V]:
		switch oKind := oChild.(type) {
		case *node[V]: // leaf, node
			return oKind.overlapsPrefixAtDepth(nKind.prefix, depth)
		case *leafNode[V]: // leaf, leaf
			return oKind.prefix.Overlaps(nKind.prefix)
		case *fringeNode[V]: // leaf, fringe
			return true
		default:
			panic("logic error, wrong node type")
		}

	case *fringeNode[V]:
		return true

	default:
		panic("logic error, wrong node type")
	}
}

// overlapsPrefixAtDepth, returns true if node overlaps with prefix
// starting with prefix octet at depth.
//
// Needed for path compressed prefix some level down in the node trie.
func (n *node[V]) overlapsPrefixAtDepth(pfx netip.Prefix, depth int) bool {
	ip := pfx.Addr()
	bits := pfx.Bits()
	octets := ip.AsSlice()
	maxDepth, lastBits := maxDepthAndLastBits(bits)

	for ; depth < len(octets); depth++ {
		if depth > maxDepth {
			break
		}

		octet := octets[depth]

		// full octet path in node trie, check overlap with last prefix octet
		if depth == maxDepth {
			return n.overlapsIdx(art.PfxToIdx(octet, lastBits))
		}

		// test if any route overlaps prefix´ so far
		// no best match needed, forward tests without backtracking
		if n.prefixes.Len() != 0 && n.lpmTest(art.OctetToIdx(octet)) {
			return true
		}

		if !n.children.Test(octet) {
			return false
		}

		// next child, node or leaf
		switch kid := n.children.MustGet(octet).(type) {
		case *node[V]:
			n = kid
			continue

		case *leafNode[V]:
			return kid.prefix.Overlaps(pfx)

		case *fringeNode[V]:
			return true

		default:
			panic("logic error, wrong node type")
		}
	}

	panic("unreachable: " + pfx.String())
}

// overlapsIdx returns true if node overlaps with prefix.
func (n *node[V]) overlapsIdx(idx uint8) bool {
	// 1. Test if any route in this node overlaps prefix?
	if n.lpmTest(uint(idx)) {
		return true
	}

	// 2. Test if prefix overlaps any route in this node

	// use bitset intersections instead of range loops
	// shallow copy pre alloted bitset for idx
	allotedPrefixRoutes := allot.IdxToPrefixRoutes(idx)
	if allotedPrefixRoutes.Intersects(&n.prefixes.BitSet256) {
		return true
	}

	// 3. Test if prefix overlaps any child in this node

	allotedHostRoutes := allot.IdxToFringeRoutes(idx)
	return allotedHostRoutes.Intersects(&n.children.BitSet256)
}
