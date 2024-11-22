// Copyright (c) 2024 Karl Gaissmaier
// SPDX-License-Identifier: MIT

package bart

import (
	"net/netip"
	"slices"

	"github.com/gaissmai/bart/internal/art"
	"github.com/gaissmai/bart/internal/lpm"
	"github.com/gaissmai/bart/internal/sparse"
)

const (
	strideLen    = 8   // byte, a multibit trie with stride len 8
	maxTreeDepth = 16  // max 16 bytes for IPv6
	maxItems     = 256 // max 256 prefixes or children in node
)

// stridePath, max 16 octets deep
type stridePath [maxTreeDepth]uint8

// node is a level node in the multibit-trie.
// A node has prefixes and children, forming the multibit trie.
//
// The prefixes, mapped by the baseIndex() function from the ART algorithm,
// form a complete binary tree.
// See the artlookup.pdf paper in the doc folder to understand the mapping function
// and the binary tree of prefixes.
//
// In contrast to the ART algorithm, sparse arrays (popcount-compressed slices)
// are used instead of fixed-size arrays.
//
// The array slots are also not pre-allocated (alloted) as described
// in the ART algorithm, fast bitset operations are used to find the
// longest-prefix-match.
//
// The child array recursively spans the trie with a branching factor of 256
// and also records path-compressed leaves in the free node slots.
type node[V any] struct {
	// prefixes contains the routes, indexed as a complete binary tree with payload V
	// with the help of the baseIndex mapping function from the ART algorithm.
	prefixes sparse.Array256[V]

	// children, recursively spans the trie with a branching factor of 256.
	children sparse.Array256[any] // [any] is a *node, with path compression a *leaf or *fringe
}

// isEmpty returns true if node has neither prefixes nor children
func (n *node[V]) isEmpty() bool {
	return n.prefixes.Len() == 0 && n.children.Len() == 0
}

// leafNode is a prefix with value, used as a path compressed child.
type leafNode[V any] struct {
	prefix netip.Prefix
	value  V
}

// fringeNode is a path-compressed leaf with value but without a prefix.
// The prefix of a fringe is solely defined by the position in the trie.
// The fringe-compressiion (no stored prefix) saves a lot of memory,
// but the algorithm is more complex.
type fringeNode[V any] struct {
	value V
}

// isFringe, leaves with /8, /16, ... /128 bits at special positions
// in the trie.
//
// Just a path-compressed leaf, inserted at the last
// possible level as path compressed (depth == maxDepth-1)
// before inserted just as a prefix in the next level down (depth == maxDepth).
//
// Nice side effect: A fringe is the default-route for all nodes below this slot!
//
//	e.g. prefix is addr/8, or addr/16, or ... addr/128
//	depth <  maxDepth-1 : a leaf, path-compressed
//	depth == maxDepth-1 : a fringe, path-compressed
//	depth == maxDepth   : a prefix with octet/pfx == 0/0 => idx == 1, a strides default route
func isFringe(depth, bits int) bool {
	maxDepth, lastBits := maxDepthAndLastBits(bits)
	return depth == maxDepth-1 && lastBits == 0
}

// cloneOrCopy, helper function,
// deep copy if v implements the Cloner interface.
func cloneOrCopy[V any](val V) V {
	if cloner, ok := any(val).(Cloner[V]); ok {
		return cloner.Clone()
	}
	// just a shallow copy
	return val
}

// cloneLeaf returns a clone of the leaf
// if the value implements the Cloner interface.
func (l *leafNode[V]) cloneLeaf() *leafNode[V] {
	return &leafNode[V]{prefix: l.prefix, value: cloneOrCopy(l.value)}
}

// cloneFringe returns a clone of the fringe
// if the value implements the Cloner interface.
func (l *fringeNode[V]) cloneFringe() *fringeNode[V] {
	return &fringeNode[V]{value: cloneOrCopy(l.value)}
}

// insertAtDepth insert a prefix/val into a node tree at depth.
// n must not be nil, prefix must be valid and already in canonical form.
func (n *node[V]) insertAtDepth(pfx netip.Prefix, val V, depth int) (exists bool) {
	ip := pfx.Addr()
	bits := pfx.Bits()
	octets := ip.AsSlice()
	maxDepth, lastBits := maxDepthAndLastBits(bits)

	// find the proper trie node to insert prefix
	// start with prefix octet at depth
	for ; depth < len(octets); depth++ {
		octet := octets[depth]

		// last masked octet: insert/override prefix/val into node
		if depth == maxDepth {
			return n.prefixes.InsertAt(art.PfxToIdx(octet, lastBits), val)
		}

		// reached end of trie path ...
		if !n.children.Test(octet) {
			// insert prefix path compressed as leaf or fringe
			if isFringe(depth, bits) {
				return n.children.InsertAt(octet, &fringeNode[V]{val})
			}
			return n.children.InsertAt(octet, &leafNode[V]{prefix: pfx, value: val})
		}

		// ... or decend down the trie
		kid := n.children.MustGet(octet)

		// kid is node or leaf at addr
		switch kid := kid.(type) {
		case *node[V]:
			n = kid
			continue // descend down to next trie level

		case *leafNode[V]:
			// reached a path compressed prefix
			// override value in slot if prefixes are equal
			if kid.prefix == pfx {
				kid.value = val
				// exists
				return true
			}

			// create new node
			// push the leaf down
			// insert new child at current leaf position (addr)
			// descend down, replace n with new child
			newNode := new(node[V])
			newNode.insertAtDepth(kid.prefix, kid.value, depth+1)

			n.children.InsertAt(octet, newNode)
			n = newNode

		case *fringeNode[V]:
			// reached a path compressed fringe
			// override value in slot if pfx is a fringe
			if isFringe(depth, bits) {
				kid.value = val
				// exists
				return true
			}

			// create new node
			// push the fringe down, it becomes a default route (idx=1)
			// insert new child at current leaf position (addr)
			// descend down, replace n with new child
			newNode := new(node[V])
			newNode.prefixes.InsertAt(1, kid.value)

			n.children.InsertAt(octet, newNode)
			n = newNode

		default:
			panic("logic error, wrong node type")
		}
	}

	panic("unreachable")
}

// insertAtDepthPersist is the immutable version of insertAtDepth.
// All visited nodes are cloned during insertion.
func (n *node[V]) insertAtDepthPersist(pfx netip.Prefix, val V, depth int) (exists bool) {
	ip := pfx.Addr()
	bits := pfx.Bits()
	octets := ip.AsSlice()
	maxDepth, lastBits := maxDepthAndLastBits(bits)

	// find the proper trie node to insert prefix
	// start with prefix octet at depth
	for ; depth < len(octets); depth++ {
		octet := octets[depth]

		// last masked octet: insert/override prefix/val into node
		if depth == maxDepth {
			return n.prefixes.InsertAt(art.PfxToIdx(octet, lastBits), val)
		}

		if !n.children.Test(octet) {
			// insert prefix path compressed as leaf or fringe
			if isFringe(depth, bits) {
				return n.children.InsertAt(octet, &fringeNode[V]{val})
			}
			return n.children.InsertAt(octet, &leafNode[V]{prefix: pfx, value: val})
		}
		kid := n.children.MustGet(octet)

		// kid is node or leaf at addr
		switch kid := kid.(type) {
		case *node[V]:
			// proceed to next level
			kid = kid.cloneFlat()
			n.children.InsertAt(octet, kid)
			n = kid
			continue // descend down to next trie level

		case *leafNode[V]:
			kid = kid.cloneLeaf()
			// reached a path compressed prefix
			// override value in slot if prefixes are equal
			if kid.prefix == pfx {
				kid.value = val
				// exists
				return true
			}

			// create new node
			// push the leaf down
			// insert new child at current leaf position (addr)
			// descend down, replace n with new child
			newNode := new(node[V])
			newNode.insertAtDepth(kid.prefix, kid.value, depth+1)

			n.children.InsertAt(octet, newNode)
			n = newNode

		case *fringeNode[V]:
			kid = kid.cloneFringe()
			// reached a path compressed fringe
			// override value in slot if pfx is a fringe
			if isFringe(depth, bits) {
				kid.value = val
				// exists
				return true
			}

			// create new node
			// push the fringe down, it becomes a default route (idx=1)
			// insert new child at current leaf position (addr)
			// descend down, replace n with new child
			newNode := new(node[V])
			newNode.prefixes.InsertAt(1, kid.value)

			n.children.InsertAt(octet, newNode)
			n = newNode

		default:
			panic("logic error, wrong node type")
		}
	}

	panic("unreachable")
}

// purgeAndCompress, purge empty nodes or compress nodes with single prefix or leaf.
func (n *node[V]) purgeAndCompress(stack []*node[V], octets []uint8, is4 bool) {
	// unwind the stack
	for depth := len(stack) - 1; depth >= 0; depth-- {
		parent := stack[depth]
		octet := octets[depth]

		pfxCount := n.prefixes.Len()
		childCount := n.children.Len()

		switch {
		case n.isEmpty():
			// just delete this empty node from parent
			parent.children.DeleteAt(octet)

		case pfxCount == 0 && childCount == 1:
			switch kid := n.children.Items[0].(type) {
			case *node[V]:
				// fast exit, we are at an intermediate path node
				// no further delete/compress upwards the stack is possible
				return
			case *leafNode[V]:
				// just one leaf, delete this node and reinsert the leaf above
				parent.children.DeleteAt(octet)

				// ... (re)insert the leaf at parents depth
				parent.insertAtDepth(kid.prefix, kid.value, depth)
			case *fringeNode[V]:
				// just one fringe, delete this node and reinsert the fringe as leaf above
				parent.children.DeleteAt(octet)

				// get the last octet back, the only item is also the first item
				lastOctet, _ := n.children.FirstSet()

				// rebuild the prefix with octets, depth, ip version and addr
				// depth is the parent's depth, so add +1 here for the kid
				fringePfx := cidrForFringe(octets, depth+1, is4, lastOctet)

				// ... (re)reinsert prefix/value at parents depth
				parent.insertAtDepth(fringePfx, kid.value, depth)
			}

		case pfxCount == 1 && childCount == 0:
			// just one prefix, delete this node and reinsert the idx as leaf above
			parent.children.DeleteAt(octet)

			// get prefix back from idx ...
			idx, _ := n.prefixes.FirstSet() // single idx must be first bit set
			val := n.prefixes.Items[0]      // single value must be at Items[0]

			// ... and octet path
			path := stridePath{}
			copy(path[:], octets)

			// depth is the parent's depth, so add +1 here for the kid
			pfx := cidrFromPath(path, depth+1, is4, idx)

			// ... (re)insert prefix/value at parents depth
			parent.insertAtDepth(pfx, val, depth)
		}

		// climb up the stack
		n = parent
	}
}

// lpmGet does a route lookup for idx in the 8-bit (stride) routing table
// at this depth and returns (baseIdx, value, true) if a matching
// longest prefix exists, or ok=false otherwise.
//
// The prefixes in the stride form a complete binary tree (CBT) using the baseIndex function.
// In contrast to the ART algorithm, I do not use an allotment approach but map
// the backtracking in the CBT by a bitset operation with a precalculated backtracking path
// for the respective idx.
func (n *node[V]) lpmGet(idx uint) (baseIdx uint8, val V, ok bool) {
	// top is the idx of the longest-prefix-match
	if top, ok := n.prefixes.IntersectionTop(lpm.BackTrackingBitset(idx)); ok {
		return top, n.prefixes.MustGet(top), true
	}

	// not found (on this level)
	return
}

// lpmTest, true if idx has a (any) longest-prefix-match in node.
// this is a contains test, faster as lookup and without value returns.
func (n *node[V]) lpmTest(idx uint) bool {
	return n.prefixes.Intersects(lpm.BackTrackingBitset(idx))
}

// cloneRec, clones the node recursive.
func (n *node[V]) cloneRec() *node[V] {
	if n == nil {
		return nil
	}

	c := new(node[V])
	if n.isEmpty() {
		return c
	}

	// shallow
	c.prefixes = *(n.prefixes.Copy())

	_, isCloner := any(*new(V)).(Cloner[V])

	// deep copy if V implements Cloner[V]
	if isCloner {
		for i, val := range c.prefixes.Items {
			c.prefixes.Items[i] = cloneOrCopy(val)
		}
	}

	// shallow
	c.children = *(n.children.Copy())

	// deep copy of nodes and leaves
	for i, kidAny := range c.children.Items {
		switch kid := kidAny.(type) {
		case *node[V]:
			// clone the child node rec-descent
			c.children.Items[i] = kid.cloneRec()
		case *leafNode[V]:
			// deep copy if V implements Cloner[V]
			c.children.Items[i] = kid.cloneLeaf()
		case *fringeNode[V]:
			// deep copy if V implements Cloner[V]
			c.children.Items[i] = kid.cloneFringe()

		default:
			panic("logic error, wrong node type")
		}
	}

	return c
}

// cloneFlat, copies the node and clone the values in prefixes and path compressed leaves
// if V implements Cloner. Used in the various ...Persist functions.
func (n *node[V]) cloneFlat() *node[V] {
	if n == nil {
		return nil
	}

	c := new(node[V])
	if n.isEmpty() {
		return c
	}

	// shallow copy
	c.prefixes = *(n.prefixes.Copy())
	c.children = *(n.children.Copy())

	if _, ok := any(*new(V)).(Cloner[V]); !ok {
		// if V doesn't implement Cloner[V], return early
		return c
	}

	// deep copy of values in prefixes
	for i, val := range c.prefixes.Items {
		c.prefixes.Items[i] = cloneOrCopy(val)
	}

	// deep copy of values in path compressed leaves
	for i, kidAny := range c.children.Items {
		switch kid := kidAny.(type) {
		case *leafNode[V]:
			c.children.Items[i] = kid.cloneLeaf()
		case *fringeNode[V]:
			c.children.Items[i] = kid.cloneFringe()
		}
	}

	return c
}

// allRec runs recursive the trie, starting at this node and
// the yield function is called for each route entry with prefix and value.
// If the yield function returns false the recursion ends prematurely and the
// false value is propagated.
//
// The iteration order is not defined, just the simplest and fastest recursive implementation.
func (n *node[V]) allRec(path stridePath, depth int, is4 bool, yield func(netip.Prefix, V) bool) bool {
	for _, idx := range n.prefixes.AsSlice(&[256]uint8{}) {
		cidr := cidrFromPath(path, depth, is4, idx)

		// callback for this prefix and val
		if !yield(cidr, n.prefixes.MustGet(idx)) {
			// early exit
			return false
		}
	}

	// for all children (nodes and leaves) in this node do ...
	for i, addr := range n.children.AsSlice(&[256]uint8{}) {
		switch kid := n.children.Items[i].(type) {
		case *node[V]:
			// rec-descent with this node
			path[depth] = addr
			if !kid.allRec(path, depth+1, is4, yield) {
				// early exit
				return false
			}
		case *leafNode[V]:
			// callback for this leaf
			if !yield(kid.prefix, kid.value) {
				// early exit
				return false
			}
		case *fringeNode[V]:
			fringePfx := cidrForFringe(path[:], depth, is4, addr)
			// callback for this fringe
			if !yield(fringePfx, kid.value) {
				// early exit
				return false
			}

		default:
			panic("logic error, wrong node type")
		}
	}

	return true
}

// allRecSorted runs recursive the trie, starting at node and
// the yield function is called for each route entry with prefix and value.
// The iteration is in prefix sort order.
//
// If the yield function returns false the recursion ends prematurely and the
// false value is propagated.
func (n *node[V]) allRecSorted(path stridePath, depth int, is4 bool, yield func(netip.Prefix, V) bool) bool {
	// get slice of all child octets, sorted by addr
	allChildAddrs := n.children.AsSlice(&[256]uint8{})

	// get slice of all indexes, sorted by idx
	allIndices := n.prefixes.AsSlice(&[256]uint8{})

	// sort indices in CIDR sort order
	slices.SortFunc(allIndices, cmpIndexRank)

	childCursor := 0

	// yield indices and childs in CIDR sort order
	for _, pfxIdx := range allIndices {
		pfxOctet, _ := art.IdxToPfx(pfxIdx)

		// yield all childs before idx
		for j := childCursor; j < len(allChildAddrs); j++ {
			childAddr := allChildAddrs[j]

			if childAddr >= pfxOctet {
				break
			}

			// yield the node (rec-descent) or leaf
			switch kid := n.children.Items[j].(type) {
			case *node[V]:
				path[depth] = childAddr
				if !kid.allRecSorted(path, depth+1, is4, yield) {
					return false
				}
			case *leafNode[V]:
				if !yield(kid.prefix, kid.value) {
					return false
				}
			case *fringeNode[V]:
				fringePfx := cidrForFringe(path[:], depth, is4, childAddr)
				// callback for this fringe
				if !yield(fringePfx, kid.value) {
					// early exit
					return false
				}

			default:
				panic("logic error, wrong node type")
			}

			childCursor++
		}

		// yield the prefix for this idx
		cidr := cidrFromPath(path, depth, is4, pfxIdx)
		// n.prefixes.Items[i] not possible after sorting allIndices
		if !yield(cidr, n.prefixes.MustGet(pfxIdx)) {
			return false
		}
	}

	// yield the rest of leaves and nodes (rec-descent)
	for j := childCursor; j < len(allChildAddrs); j++ {
		addr := allChildAddrs[j]
		switch kid := n.children.Items[j].(type) {
		case *node[V]:
			path[depth] = addr
			if !kid.allRecSorted(path, depth+1, is4, yield) {
				return false
			}
		case *leafNode[V]:
			if !yield(kid.prefix, kid.value) {
				return false
			}
		case *fringeNode[V]:
			fringePfx := cidrForFringe(path[:], depth, is4, addr)
			// callback for this fringe
			if !yield(fringePfx, kid.value) {
				// early exit
				return false
			}

		default:
			panic("logic error, wrong node type")
		}
	}

	return true
}

// unionRec combines two nodes, changing the receiver node.
// If there are duplicate entries, the value is taken from the other node.
// Count duplicate entries to adjust the t.size struct members.
// The values are cloned before merging.
func (n *node[V]) unionRec(o *node[V], depth int) (duplicates int) {
	// for all prefixes in other node do ...
	for i, oIdx := range o.prefixes.AsSlice(&[256]uint8{}) {
		// clone/copy the value from other node at idx
		clonedVal := cloneOrCopy(o.prefixes.Items[i])

		// insert/overwrite cloned value from o into n
		if n.prefixes.InsertAt(oIdx, clonedVal) {
			// this prefix is duplicate in n and o
			duplicates++
		}
	}

	// for all child addrs in other node do ...
	for i, addr := range o.children.AsSlice(&[256]uint8{}) {
		//  12 possible combinations to union this child and other child
		//
		//  THIS,   OTHER: (always clone the other kid!)
		//  --------------
		//  NULL,   node    <-- insert node at addr
		//  NULL,   leaf    <-- insert leaf at addr
		//  NULL,   fringe  <-- insert fringe at addr

		//  node,   node    <-- union rec-descent with node
		//  node,   leaf    <-- insert leaf at depth+1
		//  node,   fringe  <-- insert fringe at depth+1

		//  leaf,   node    <-- insert new node, push this leaf down, union rec-descent
		//  leaf,   leaf    <-- insert new node, push both leaves down (!first check equality)
		//  leaf,   fringe  <-- insert new node, push this leaf and fringe down

		//  fringe, node    <-- insert new node, push this fringe down, union rec-descent
		//  fringe, leaf    <-- insert new node, push this fringe down, insert other leaf at depth+1
		//  fringe, fringe  <-- just overwrite value
		//
		// try to get child at same addr from n
		thisChild, thisExists := n.children.Get(addr)
		if !thisExists { // NULL, ... slot at addr is empty
			switch otherKid := o.children.Items[i].(type) {
			case *node[V]: // NULL, node
				n.children.InsertAt(addr, otherKid.cloneRec())
				continue

			case *leafNode[V]: // NULL, leaf
				n.children.InsertAt(addr, otherKid.cloneLeaf())
				continue

			case *fringeNode[V]: // NULL, fringe
				n.children.InsertAt(addr, otherKid.cloneFringe())
				continue

			default:
				panic("logic error, wrong node type")
			}
		}

		switch thisKid := thisChild.(type) {
		case *node[V]: // node, ...
			switch otherKid := o.children.Items[i].(type) {
			case *node[V]: // node, node
				// both childs have node at addr, call union rec-descent on child nodes
				duplicates += thisKid.unionRec(otherKid.cloneRec(), depth+1)
				continue

			case *leafNode[V]: // node, leaf
				// push this cloned leaf down, count duplicate entry
				clonedLeaf := otherKid.cloneLeaf()
				if thisKid.insertAtDepth(clonedLeaf.prefix, clonedLeaf.value, depth+1) {
					duplicates++
				}
				continue

			case *fringeNode[V]: // node, fringe
				// push this fringe down, a fringe becomes a default route one level down
				clonedFringe := otherKid.cloneFringe()
				if thisKid.prefixes.InsertAt(1, clonedFringe.value) {
					duplicates++
				}
				continue
			}

		case *leafNode[V]: // leaf, ...
			switch otherKid := o.children.Items[i].(type) {
			case *node[V]: // leaf, node
				// create new node
				nc := new(node[V])

				// push this leaf down
				nc.insertAtDepth(thisKid.prefix, thisKid.value, depth+1)

				// insert the new node at current addr
				n.children.InsertAt(addr, nc)

				// unionRec this new node with other kid node
				duplicates += nc.unionRec(otherKid.cloneRec(), depth+1)
				continue

			case *leafNode[V]: // leaf, leaf
				// shortcut, prefixes are equal
				if thisKid.prefix == otherKid.prefix {
					thisKid.value = cloneOrCopy(otherKid.value)
					duplicates++
					continue
				}

				// create new node
				nc := new(node[V])

				// push this leaf down
				nc.insertAtDepth(thisKid.prefix, thisKid.value, depth+1)

				// insert at depth cloned leaf, maybe duplicate
				clonedLeaf := otherKid.cloneLeaf()
				if nc.insertAtDepth(clonedLeaf.prefix, clonedLeaf.value, depth+1) {
					duplicates++
				}

				// insert the new node at current addr
				n.children.InsertAt(addr, nc)
				continue

			case *fringeNode[V]: // leaf, fringe
				// create new node
				nc := new(node[V])

				// push this leaf down
				nc.insertAtDepth(thisKid.prefix, thisKid.value, depth+1)

				// push this cloned fringe down, it becomes the default route
				clonedFringe := otherKid.cloneFringe()
				if nc.prefixes.InsertAt(1, clonedFringe.value) {
					duplicates++
				}

				// insert the new node at current addr
				n.children.InsertAt(addr, nc)
				continue
			}

		case *fringeNode[V]: // fringe, ...
			switch otherKid := o.children.Items[i].(type) {
			case *node[V]: // fringe, node
				// create new node
				nc := new(node[V])

				// push this fringe down, it becomes the default route
				nc.prefixes.InsertAt(1, thisKid.value)

				// insert the new node at current addr
				n.children.InsertAt(addr, nc)

				// unionRec this new node with other kid node
				duplicates += nc.unionRec(otherKid.cloneRec(), depth+1)
				continue

			case *leafNode[V]: // fringe, leaf
				// create new node
				nc := new(node[V])

				// push this fringe down, it becomes the default route
				nc.prefixes.InsertAt(1, thisKid.value)

				// push this cloned leaf down
				clonedLeaf := otherKid.cloneLeaf()
				if nc.insertAtDepth(clonedLeaf.prefix, clonedLeaf.value, depth+1) {
					duplicates++
				}

				// insert the new node at current addr
				n.children.InsertAt(addr, nc)
				continue

			case *fringeNode[V]: // fringe, fringe
				thisKid.value = otherKid.cloneFringe().value
				duplicates++
				continue
			}

		default:
			panic("logic error, wrong node type")
		}
	}

	return duplicates
}

// eachLookupPrefix does an all prefix match in the 8-bit (stride) routing table
// at this depth and calls yield() for any matching CIDR.
func (n *node[V]) eachLookupPrefix(octets []byte, depth int, is4 bool, pfxIdx uint, yield func(netip.Prefix, V) bool) (ok bool) {
	// path needed below more than once in loop
	var path stridePath
	copy(path[:], octets)

	// fast forward, it's a /8 route, too big for bitset256
	if pfxIdx > 255 {
		pfxIdx >>= 1
	}
	idx := uint8(pfxIdx) // now it fits into uint8

	for ; idx > 0; idx >>= 1 {
		if n.prefixes.Test(idx) {
			val := n.prefixes.MustGet(idx)
			cidr := cidrFromPath(path, depth, is4, idx)

			if !yield(cidr, val) {
				return false
			}
		}
	}

	return true
}

// eachSubnet calls yield() for any covered CIDR by parent prefix in natural CIDR sort order.
func (n *node[V]) eachSubnet(octets []byte, depth int, is4 bool, pfxIdx uint8, yield func(netip.Prefix, V) bool) bool {
	// octets as array, needed below more than once
	var path stridePath
	copy(path[:], octets)

	pfxFirstAddr, pfxLastAddr := art.IdxToRange(pfxIdx)

	allCoveredIndices := make([]uint8, 0, maxItems)
	for _, idx := range n.prefixes.AsSlice(&[256]uint8{}) {
		thisFirstAddr, thisLastAddr := art.IdxToRange(idx)

		if thisFirstAddr >= pfxFirstAddr && thisLastAddr <= pfxLastAddr {
			allCoveredIndices = append(allCoveredIndices, idx)
		}
	}

	// sort indices in CIDR sort order
	slices.SortFunc(allCoveredIndices, cmpIndexRank)

	// 2. collect all covered child addrs by prefix

	allCoveredChildAddrs := make([]uint8, 0, maxItems)
	for _, addr := range n.children.AsSlice(&[256]uint8{}) {
		if addr >= pfxFirstAddr && addr <= pfxLastAddr {
			allCoveredChildAddrs = append(allCoveredChildAddrs, addr)
		}
	}

	// 3. yield covered indices, pathcomp prefixes and childs in CIDR sort order

	addrCursor := 0

	// yield indices and childs in CIDR sort order
	for _, pfxIdx := range allCoveredIndices {
		pfxOctet, _ := art.IdxToPfx(pfxIdx)

		// yield all childs before idx
		for j := addrCursor; j < len(allCoveredChildAddrs); j++ {
			addr := allCoveredChildAddrs[j]
			if addr >= pfxOctet {
				break
			}

			// yield the node or leaf?
			switch kid := n.children.MustGet(addr).(type) {
			case *node[V]:
				path[depth] = addr
				if !kid.allRecSorted(path, depth+1, is4, yield) {
					return false
				}

			case *leafNode[V]:
				if !yield(kid.prefix, kid.value) {
					return false
				}

			case *fringeNode[V]:
				fringePfx := cidrForFringe(path[:], depth, is4, addr)
				// callback for this fringe
				if !yield(fringePfx, kid.value) {
					// early exit
					return false
				}

			default:
				panic("logic error, wrong node type")
			}

			addrCursor++
		}

		// yield the prefix for this idx
		cidr := cidrFromPath(path, depth, is4, pfxIdx)
		// n.prefixes.Items[i] not possible after sorting allIndices
		if !yield(cidr, n.prefixes.MustGet(pfxIdx)) {
			return false
		}
	}

	// yield the rest of leaves and nodes (rec-descent)
	for _, addr := range allCoveredChildAddrs[addrCursor:] {
		// yield the node or leaf?
		switch kid := n.children.MustGet(addr).(type) {
		case *node[V]:
			path[depth] = addr
			if !kid.allRecSorted(path, depth+1, is4, yield) {
				return false
			}
		case *leafNode[V]:
			if !yield(kid.prefix, kid.value) {
				return false
			}
		case *fringeNode[V]:
			fringePfx := cidrForFringe(path[:], depth, is4, addr)
			// callback for this fringe
			if !yield(fringePfx, kid.value) {
				// early exit
				return false
			}

		default:
			panic("logic error, wrong node type")
		}
	}

	return true
}

// cmpIndexRank, sort indexes in prefix sort order.
func cmpIndexRank(aIdx, bIdx uint8) int {
	// convert idx [1..255] to prefix
	aOctet, aBits := art.IdxToPfx(aIdx)
	bOctet, bBits := art.IdxToPfx(bIdx)

	// cmp the prefixes, first by address and then by bits
	if aOctet == bOctet {
		if aBits <= bBits {
			return -1
		}

		return 1
	}

	if aOctet < bOctet {
		return -1
	}

	return 1
}

// cidrFromPath, helper function,
// get prefix back from stride path, depth and idx.
// The prefix is solely defined by the position in the trie and the baseIndex.
func cidrFromPath(path stridePath, depth int, is4 bool, idx uint8) netip.Prefix {
	octet, pfxLen := art.IdxToPfx(idx)

	// set masked byte in path at depth
	path[depth] = octet

	// zero/mask the bytes after prefix bits
	clear(path[depth+1:])

	// make ip addr from octets
	var ip netip.Addr
	if is4 {
		ip = netip.AddrFrom4([4]byte(path[:4]))
	} else {
		ip = netip.AddrFrom16(path)
	}

	// calc bits with pathLen and pfxLen
	bits := depth<<3 + int(pfxLen)

	// return a normalized prefix from ip/bits
	return netip.PrefixFrom(ip, bits)
}

// cidrForFringe, helper function,
// get prefix back from octets path, depth, IP version and last octet.
// The prefix of a fringe is solely defined by the position in the trie.
func cidrForFringe(octets []byte, depth int, is4 bool, lastOctet uint8) netip.Prefix {
	path := stridePath{}
	copy(path[:], octets[:depth+1])

	// replace last octet
	path[depth] = lastOctet

	// make ip addr from octets
	var ip netip.Addr
	if is4 {
		ip = netip.AddrFrom4([4]byte(path[:4]))
	} else {
		ip = netip.AddrFrom16(path)
	}

	// it's a fringe, bits are alway /8, /16, /24, ...
	bits := (depth + 1) << 3

	// return a (normalized) prefix from ip/bits
	return netip.PrefixFrom(ip, bits)
}
