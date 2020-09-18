package builder

// Heap is a binary tree where parent node is "smaller" than its children nodes ("Heap Property").
// Heap is stored in a slice, where children nodes for node at position ix are at positions 2*ix+1 and 2*ix+2.
//
// Heapify will maintain the heap property for node "ix".
//
// Building the heap for the first time must go from the latest element (last leaf) towards the element 0 (root of the tree).
// Once built, first element is the smallest.
//
// Element "ix" can be removed from the heap by moving the last element to position "ix", shrinking the heap
// and restoring the heap property from index "ix". (This is typically done from root, ix=0).
func heapify(length int, ix int, less func(i, j int) bool, swap func(i, j int)) {
	smallest := ix
	left := 2*ix + 1
	right := 2*ix + 2

	if left < length && less(left, smallest) {
		smallest = left
	}

	if right < length && less(right, smallest) {
		smallest = right
	}

	if smallest != ix {
		swap(ix, smallest)
		heapify(length, smallest, less, swap)
	}
}
