package stringutil

import (
	"container/heap"
)

func KWayMerge(arrays ...[]string) []string {
	k := len(arrays)
	if k == 0 {
		return []string{}
	}
	h := MinHeap{nodes: make([]*Node, 0, k), valMap: make(map[string]struct{}, k)}
	result := make([]string, 0, len(arrays[0]))

	// Initialize the h with the first element from each array
	for i, arr := range arrays {
		if len(arr) > 0 {
			h.nodes = append(h.nodes, &Node{arr[0], i, 0})
		}
	}
	heap.Init(&h)

	// Keep popping elements from the h and adding to the result
	for h.Len() > 0 {
		node := h.nodes[0]
		if len(result) == 0 || result[len(result)-1] != node.val {
			result = append(result, node.val)
		}

		remove := true
		// If there are more elements in the same array, add to h
		for node.idx < len(arrays[node.array])-1 {
			next := arrays[node.array][node.idx+1]
			node.val = next
			node.idx += 1
			if !h.Contains(next) {
				heap.Fix(&h, 0)
				remove = false
				break
			}
		}

		if remove {
			heap.Pop(&h)
		}
	}

	return result
}

// Node struct represents an element in the heap
type Node struct {
	val   string
	array int
	idx   int
}

// MinHeap is a priority queue that stores Nodes in increasing order of val
type MinHeap struct {
	nodes  []*Node
	valMap map[string]struct{}
}

func (h MinHeap) Len() int           { return len(h.nodes) }
func (h MinHeap) Less(i, j int) bool { return h.nodes[i].val < h.nodes[j].val }
func (h MinHeap) Swap(i, j int)      { h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i] }

func (h *MinHeap) Push(x interface{}) {
	h.nodes = append(h.nodes, x.(*Node))
	h.valMap[x.(*Node).val] = struct{}{}
}

func (h MinHeap) Contains(x string) bool {
	_, ok := h.valMap[x]
	return ok
}

func (h *MinHeap) Pop() interface{} {
	old := h.nodes
	n := len(old)
	node := old[n-1]
	h.nodes = old[:n-1]
	delete(h.valMap, node.val)
	return node
}
