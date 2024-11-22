// Copyright (c) 2024 Karl Gaissmaier
// SPDX-License-Identifier: MIT

package bart

import (
	"net/netip"

	"github.com/gaissmai/bart/internal/art"
)

// InsertPersist is similar to Insert but the receiver isn't modified.
//
// All nodes touched during insert are cloned and a new Table is returned.
// This is not a full [Table.Clone], all untouched nodes are still referenced
// from both Tables.
//
// If the payload V is a pointer or contains a pointer, it should
// implement the cloner interface.
//
// This is orders of magnitude slower than Insert (μsec versus nsec).
//
// The bulk table load should be done with [Table.Insert] and then you can
// use InsertPersist, [Table.UpdatePersist] and [Table.DeletePersist] for lock-free updates.
func (t *Table[V]) InsertPersist(pfx netip.Prefix, val V) *Table[V] {
	if !pfx.IsValid() {
		return t
	}

	pt := &Table[V]{
		root4: t.root4,
		root6: t.root6,
		size4: t.size4,
		size6: t.size6,
	}

	// canonicalize prefix
	pfx = pfx.Masked()

	is4 := pfx.Addr().Is4()

	n := pt.rootNodeByVersion(is4)

	// clone the root of insertion path
	*n = *n.cloneFlat()

	// clone nodes along the insertion path
	if n.insertAtDepthPersist(pfx, val, 0) {
		// prefix existed, no size increment
		return pt
	}

	// true insert, update size
	pt.sizeUpdate(is4, 1)

	return pt
}

// UpdatePersist is similar to Update but the receiver isn't modified.
//
// All nodes touched during update are cloned and a new Table is returned.
// This is not a full [Table.Clone], all untouched nodes are still referenced
// from both Tables.
//
// If the payload V is a pointer or contains a pointer, it should
// implement the cloner interface.
//
// This is orders of magnitude slower than Update (μsec versus nsec).
func (t *Table[V]) UpdatePersist(pfx netip.Prefix, cb func(val V, ok bool) V) (pt *Table[V], newVal V) {
	var zero V

	if !pfx.IsValid() {
		return t, zero
	}

	// canonicalize prefix
	pfx = pfx.Masked()

	// values derived from pfx
	ip := pfx.Addr()
	is4 := ip.Is4()
	bits := pfx.Bits()

	pt = &Table[V]{
		root4: t.root4,
		root6: t.root6,
		size4: t.size4,
		size6: t.size6,
	}

	n := pt.rootNodeByVersion(is4)

	// clone the root of insertion path
	*n = *(n.cloneFlat())

	maxDepth, lastBits := maxDepthAndLastBits(bits)

	octets := ip.AsSlice()

	// find the proper trie node to update prefix
	for depth, octet := range octets {
		// last octet from prefix, update/insert prefix into node
		if depth == maxDepth {
			newVal, exists := n.prefixes.UpdateAt(art.PfxToIdx(octet, lastBits), cb)
			if !exists {
				pt.sizeUpdate(is4, 1)
			}
			return pt, newVal
		}

		addr := octet

		// go down in tight loop to last octet
		if !n.children.Test(addr) {
			// insert prefix path compressed
			newVal := cb(zero, false)
			if isFringe(depth, bits) {
				n.children.InsertAt(addr, &fringeNode[V]{value: newVal})
			} else {
				n.children.InsertAt(addr, &leafNode[V]{prefix: pfx, value: newVal})
			}

			pt.sizeUpdate(is4, 1)
			return pt, newVal
		}
		kid := n.children.MustGet(addr)

		// kid is node or leaf at addr
		switch kid := kid.(type) {
		case *node[V]:
			// proceed to next level
			kid = kid.cloneFlat()
			n.children.InsertAt(addr, kid)
			n = kid
			continue // descend down to next trie level

		case *leafNode[V]:
			kid = kid.cloneLeaf()

			// update existing value if prefixes are equal
			if kid.prefix == pfx {
				newVal = cb(kid.value, true)
				n.children.InsertAt(addr, &leafNode[V]{prefix: pfx, value: newVal})

				return pt, newVal
			}

			// create new node
			// push the leaf down
			// insert new child at current leaf position (addr)
			// descend down, replace n with new child
			newNode := new(node[V])
			newNode.insertAtDepth(kid.prefix, kid.value, depth+1)

			n.children.InsertAt(addr, newNode)
			n = newNode

		case *fringeNode[V]:
			kid = kid.cloneFringe()

			// update existing value if prefix is fringe
			if isFringe(depth, bits) {
				newVal = cb(kid.value, true)
				n.children.InsertAt(addr, &fringeNode[V]{value: newVal})
				return pt, newVal
			}

			// create new node
			// push the fringe down, it becomes a default route (idx=1)
			// insert new child at current leaf position (addr)
			// descend down, replace n with new child
			newNode := new(node[V])
			newNode.prefixes.InsertAt(1, kid.value)

			n.children.InsertAt(addr, newNode)
			n = newNode

		default:
			panic("logic error, wrong node type")
		}
	}

	panic("unreachable")
}

// DeletePersist is similar to Delete but the receiver isn't modified.
// All nodes touched during delete are cloned and a new Table is returned.
//
// This is orders of magnitude slower than Delete (μsec versus nsec).
func (t *Table[V]) DeletePersist(pfx netip.Prefix) *Table[V] {
	pt, _, _ := t.getAndDeletePersist(pfx)
	return pt
}

// GetAndDeletePersist is similar to GetAndDelete but the receiver isn't modified.
// All nodes touched during delete are cloned and a new Table is returned.
//
// If the payload V is a pointer or contains a pointer, it should
// implement the cloner interface.
//
// This is orders of magnitude slower than GetAndDelete (μsec versus nsec).
func (t *Table[V]) GetAndDeletePersist(pfx netip.Prefix) (pt *Table[V], val V, ok bool) {
	return t.getAndDeletePersist(pfx)
}

// getAndDeletePersist is similar to getAndDelete but the receiver isn't modified.
func (t *Table[V]) getAndDeletePersist(pfx netip.Prefix) (pt *Table[V], val V, exists bool) {
	if !pfx.IsValid() {
		return t, val, false
	}

	// canonicalize prefix
	pfx = pfx.Masked()

	// values derived from pfx
	ip := pfx.Addr()
	is4 := ip.Is4()
	bits := pfx.Bits()

	pt = &Table[V]{
		root4: t.root4,
		root6: t.root6,
		size4: t.size4,
		size6: t.size6,
	}

	n := pt.rootNodeByVersion(is4)

	// clone the root of insertion path
	*n = *n.cloneFlat()

	maxDepth, lastBits := maxDepthAndLastBits(bits)

	octets := ip.AsSlice()

	// record path to deleted node
	// needed to purge and/or path compress nodes after deletion
	stack := [maxTreeDepth]*node[V]{}

	// find the trie node
	for depth, octet := range octets {
		// push cloned node on stack for path recording
		stack[depth] = n

		if depth == maxDepth {
			// try to delete prefix in trie node
			val, exists = n.prefixes.DeleteAt(art.PfxToIdx(octet, lastBits))
			if !exists {
				// nothing to delete
				return pt, val, false
			}

			pt.sizeUpdate(is4, -1)
			n.purgeAndCompress(stack[:depth], octets, is4)
			return pt, val, exists
		}

		addr := octet
		if !n.children.Test(addr) {
			// nothing to delete
			return pt, val, false
		}
		kid := n.children.MustGet(addr)

		// kid is node or leaf at addr
		switch kid := kid.(type) {
		case *node[V]:
			// proceed to next level
			kid = kid.cloneFlat()
			n.children.InsertAt(addr, kid)
			n = kid
			continue // descend down to next trie level

		case *fringeNode[V]:
			kid = kid.cloneFringe()

			// reached a path compressed fringe, stop traversing
			if !isFringe(depth, bits) {
				// nothing to delete
				return pt, val, false
			}

			// prefix is equal fringe, delete fringe
			n.children.DeleteAt(addr)

			pt.sizeUpdate(is4, -1)
			n.purgeAndCompress(stack[:depth], octets, is4)

			// kid.value is cloned
			return pt, kid.value, true

		case *leafNode[V]:
			kid = kid.cloneLeaf()

			// reached a path compressed prefix, stop traversing
			if kid.prefix != pfx {
				// nothing to delete
				return pt, val, false
			}

			// prefix is equal leaf, delete leaf
			n.children.DeleteAt(addr)

			pt.sizeUpdate(is4, -1)
			n.purgeAndCompress(stack[:depth], octets, is4)

			// kid.value is cloned
			return pt, kid.value, true

		default:
			panic("logic error, wrong node type")
		}
	}

	panic("unreachable")
}
