package ring

import (
	"errors"
	"fmt"
	"sort"
)

// TokenNavigator provides utility methods for traversing the ring.
type TokenNavigator []TokenDesc

// findTokenIndex searches the slice of tokens for the index of
// the token matching the value provided by the token parameter.
func (n TokenNavigator) findTokenIndex(token uint32) (int, bool) {
	i := sort.Search(len(n), func(x int) bool {
		return n[x].Token >= token
	})
	if i >= len(n) {
		i = 0
	}
	return i, len(n) > i && n[i].Token == token
}

// SetIngesterTokens updates the tokens for a specific ingester with the provided
// tokens. SetIngesterTokens will remove tokens for id that are no longer in the
// list provided by tokens.
func (n *TokenNavigator) SetIngesterTokens(id string, tokens []uint32) {
	oldTokens := *n
	newTokens := make([]TokenDesc, 0, len(oldTokens))

	// Copy all tokens from other ingesters
	for _, tok := range oldTokens {
		if tok.Ingester != id {
			newTokens = append(newTokens, tok)
			continue
		}
	}

	// Add back in our tokens
	for _, tok := range tokens {
		newTokens = append(newTokens, TokenDesc{Token: tok, Ingester: id})
	}

	// Re-sort the list
	sort.Sort(ByToken(newTokens))
	*n = newTokens
}

// HealthCheckFunc is a function that validates whether a given token in the
// ring is healthy and can be used.
type HealthCheckFunc func(TokenDesc) bool

// Neighbor moves around the token list to find the ingester neighboring start determined
// by the provided offset. The healthy function determines which tokens are considered as
// potential neighbors.
//
// Only one token per ingester is considered as a neighbor. If markStartIngesterSeen is
// true, then the ingester for the starting token is included as one of the seen
// ingesters and that ingester won't be considered again.
//
// If offset has a positive value, Neighbor searches the ring clockwise.
// Otherwise, if offset has a negative value, Neighbor searches the ring counter-clockwise.
// An offset of 0 will return the starting token.
func (n TokenNavigator) Neighbor(start uint32, offset int, markStartIngesterSeen bool, healthy HealthCheckFunc) (TokenDesc, error) {
	idx, ok := n.findTokenIndex(start)
	if !ok {
		return TokenDesc{}, fmt.Errorf("could not find token %d in ring", start)
	}
	if offset == 0 {
		return n[idx], nil
	}

	distinct := map[string]bool{}
	if markStartIngesterSeen {
		distinct[n[idx].Ingester] = true
	}

	numNeighbors := offset
	neighborCount := 0

	direction := 1

	if offset < 0 {
		direction = -1
		numNeighbors = -numNeighbors
	}

	it := newTokenIterator(n, direction, idx, healthy)
	for it.HasNext() {
		successor := it.Next()
		if distinct[successor.Ingester] {
			continue
		}

		neighborCount++
		if neighborCount == numNeighbors {
			return successor, nil
		}

		distinct[successor.Ingester] = true
	}

	return TokenDesc{}, fmt.Errorf("could not find neighbor %d for token %d", offset, start)
}

// Predecessors finds all tokens of which the start is the offset-th neighbor to.
func (n TokenNavigator) Predecessors(start uint32, offset int, healthy HealthCheckFunc) ([]TokenDesc, error) {
	idx, ok := n.findTokenIndex(start)
	if !ok {
		return nil, fmt.Errorf("could not find token %d in ring", start)
	} else if offset == 0 {
		return []TokenDesc{n[idx]}, nil
	} else if offset < 0 {
		return nil, errors.New("Predecessors must be called with a non-negative offset")
	}

	var (
		predecessors []TokenDesc

		distinct = map[string]bool{}
		startTok = n[idx]
	)

	// We'll be checking to see if a successor is our starting token,
	// so we force our starting token to be considered healthy here.
	healthyOrStart := func(t TokenDesc) bool {
		if t.Token == startTok.Token {
			return true
		}
		return healthy(t)
	}

	it := newTokenIterator(n, -1, idx, healthyOrStart)
	for it.HasNext() {
		predecessor := it.Next()

		// Stop if this is a new ingester and we already have seen enough unique ingesters.
		if distinct[predecessor.Ingester] && len(distinct) == offset+1 {
			break
		}

		// Collect the token if its successor is our starting token.
		succ, err := n.Neighbor(predecessor.Token, offset, true, healthyOrStart)
		if err != nil {
			return predecessors, err
		} else if succ.Token == start {
			predecessors = append([]TokenDesc{predecessor}, predecessors...)
		}

		distinct[predecessor.Ingester] = true
	}

	return predecessors, nil
}

// RangeOptions configures the search parameters of Desc.InRange.
type RangeOptions struct {
	// Range of tokens to search.
	Range TokenRange

	// ID is the ingester ID to search for.
	ID string

	// IncludeFrom will include the From token in the range as part of the search.
	IncludeFrom bool

	// IncludeTo will include the To token in the range as part of the search.
	IncludeTo bool
}

// InRange checks to see if a given ingester ID (specified by opts.ID) has any
// tokens within the range specified by opts.Range. The inclusivity of each side
// of the range is determined by opts.IncludeFrom and opts.IncludeTo.
func (n TokenNavigator) InRange(opts RangeOptions, healthy HealthCheckFunc) (bool, error) {
	start, ok := n.findTokenIndex(opts.Range.From)
	if !ok {
		return false, fmt.Errorf("could not find token %d in ring", opts.Range.From)
	}

	end, ok := n.findTokenIndex(opts.Range.To)
	if !ok {
		return false, fmt.Errorf("could not find token %d in ring", opts.Range.To)
	}

	var (
		startTok = n[start]
		endTok   = n[end]
	)

	if opts.IncludeFrom && startTok.Ingester == opts.ID && healthy(startTok) {
		return true, nil
	} else if startTok.Token == endTok.Token {
		return opts.IncludeTo && endTok.Ingester == opts.ID && healthy(endTok), nil
	}

	// We don't pass a health check function to the iterator to make sure we don't
	// accidentally iterate past an unhealthy end token.
	it := newTokenIterator(n, 1, start, nil)
	for it.HasNext() {
		tok := it.Next()
		if tok.Token == endTok.Token {
			return opts.IncludeTo && tok.Ingester == opts.ID && healthy(tok), nil
		} else if tok.Ingester == opts.ID && healthy(tok) {
			return true, nil
		}
	}

	return false, nil
}

// tokenIterator allows for iterating through tokens in the ring. tokenIterator
// will only return healthy tokens. If healthy is not defined, all tokens
// are considered healthy.
type tokenIterator struct {
	tokens    []TokenDesc
	start     int
	healthy   HealthCheckFunc
	direction int

	idx  int
	next *TokenDesc
}

func newTokenIterator(tokens []TokenDesc, direction int, start int, healthy HealthCheckFunc) tokenIterator {
	return tokenIterator{tokens: tokens, direction: direction, start: start, healthy: healthy, idx: start}
}

func (it *tokenIterator) HasNext() bool {
	for {
		it.idx += it.direction
		if it.idx < 0 {
			it.idx = len(it.tokens) - 1
		} else {
			it.idx %= len(it.tokens)
		}

		if it.idx == it.start {
			it.next = nil
			return false
		}
		tok := it.tokens[it.idx]
		if it.healthy == nil || it.healthy(tok) {
			it.next = &tok
			return true
		}
	}
}

func (it *tokenIterator) Next() TokenDesc {
	if it.next == nil {
		return TokenDesc{}
	}
	return *it.next
}
