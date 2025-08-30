package plan_fragments

import "github.com/thanos-io/promql-engine/logicalplan"

// Fragmenter interface
type Fragmenter interface {
	// Fragment function fragments the logical query plan and will always return the fragment in the order of child-to-root
	// in other words, the order of the fragment in the array will be the order they are being scheduled
	Fragment(node logicalplan.Node) ([]Fragment, error)
}

type DummyFragmenter struct {
}

func (f *DummyFragmenter) Fragment(node logicalplan.Node) ([]Fragment, error) {
	// simple logic without distributed optimizer
	return []Fragment{
		{
			Node:       node,
			FragmentID: uint64(1),
			ChildIDs:   []uint64{},
			IsRoot:     true,
		},
	}, nil
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

func NewDummyFragmenter() Fragmenter {
	return &DummyFragmenter{}
}
