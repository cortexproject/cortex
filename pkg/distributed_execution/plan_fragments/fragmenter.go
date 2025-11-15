package plan_fragments

import (
	"encoding/binary"

	"github.com/google/uuid"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/cortexproject/cortex/pkg/distributed_execution"
)

// Fragmenter interface
type Fragmenter interface {
	// Fragment function fragments the logical query plan and will always return the fragment in the order of child-to-root
	// in other words, the order of the fragment in the array will be the order they are being scheduled
	Fragment(queryID uint64, node logicalplan.Node) ([]Fragment, error)
}

func getNewID() uint64 {
	id := uuid.New()
	return binary.BigEndian.Uint64(id[:8])
}

type PlanFragmenter struct {
}

func (f *PlanFragmenter) Fragment(queryID uint64, node logicalplan.Node) ([]Fragment, error) {
	fragments := []Fragment{}

	nodeToFragmentID := make(map[*logicalplan.Node]uint64)
	nodeToSubtreeFragmentIDs := make(map[*logicalplan.Node][]uint64)

	logicalplan.TraverseBottomUp(nil, &node, func(parent, current *logicalplan.Node) bool {
		childFragmentIDs := make(map[uint64]bool)
		children := (*current).Children()

		for _, child := range children {
			if subtreeIDs, exists := nodeToSubtreeFragmentIDs[child]; exists {
				for _, fragmentID := range subtreeIDs {
					childFragmentIDs[fragmentID] = true
				}
			}
		}

		childIDs := make([]uint64, 0, len(childFragmentIDs))
		for fragmentID := range childFragmentIDs {
			childIDs = append(childIDs, fragmentID)
		}

		if parent == nil { // root fragment
			newFragment := Fragment{
				Node:       *current,
				FragmentID: getNewID(),
				ChildIDs:   childIDs,
				IsRoot:     true,
			}
			fragments = append(fragments, newFragment)

			// cache subtree fragment IDs for this node
			nodeToSubtreeFragmentIDs[current] = childIDs

		} else if distributed_execution.RemoteNode == (*current).Type() {
			remoteNode := (*current).(*distributed_execution.Remote)
			fragmentID := getNewID()
			nodeToFragmentID[current] = fragmentID

			// Set the fragment key for the remote node
			key := distributed_execution.MakeFragmentKey(queryID, fragmentID)
			remoteNode.FragmentKey = key

			newFragment := Fragment{
				Node:       remoteNode.Expr,
				FragmentID: fragmentID,
				ChildIDs:   childIDs,
				IsRoot:     false,
			}

			fragments = append(fragments, newFragment)

			subtreeIDs := []uint64{fragmentID}
			nodeToSubtreeFragmentIDs[current] = subtreeIDs
		} else {
			nodeToSubtreeFragmentIDs[current] = childIDs
		}

		return false
	})

	if len(fragments) > 0 {
		return fragments, nil
	}

	// for non-query API calls
	// --> treat as root fragment and immediately return the result
	return []Fragment{{
		Node:       node,
		FragmentID: uint64(0),
		ChildIDs:   []uint64{},
		IsRoot:     true,
	}}, nil
}

type Fragment struct {
	Node       logicalplan.Node
	FragmentID uint64
	ChildIDs   []uint64
	IsRoot     bool
}

func (s *Fragment) IsEmpty() bool {
	if s.Node != nil {
		return false
	}
	if s.FragmentID != 0 {
		return false
	}
	if s.IsRoot {
		return false
	}
	if len(s.ChildIDs) != 0 {
		return false
	}
	return true
}

func NewPlanFragmenter() Fragmenter {
	return &PlanFragmenter{}
}
