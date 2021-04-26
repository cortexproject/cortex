package merger

// Merger represents logic for merging response bodies.
type Merger interface {
	MergeResponses([][]byte) ([]byte, error)
}

// Noop is an implementation of the Merger interface which does not actually merge
// responses, but just returns an arbitrary response(the first in the list). It can
// be used for write requests where the response is either empty or inconsequential.
type Noop struct{}

func (Noop) MergeResponses(in [][]byte) ([]byte, error) {
	return in[0], nil
}
