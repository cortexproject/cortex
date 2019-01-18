package ring

import (
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
)

// ByToken is a sortable list of TokenDescs
type ByToken []TokenDesc

func (ts ByToken) Len() int           { return len(ts) }
func (ts ByToken) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts ByToken) Less(i, j int) bool { return ts[i].Token < ts[j].Token }

// ProtoDescFactory makes new Descs
func ProtoDescFactory() proto.Message {
	return NewDesc()
}

// NewDesc returns an empty ring.Desc
func NewDesc() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{},
	}
}

// AddIngester adds the given ingester to the ring.
func (d *Desc) AddIngester(id, addr string, tokens []uint32, state IngesterState, normaliseTokens bool) {
	if d.Ingesters == nil {
		d.Ingesters = map[string]IngesterDesc{}
	}

	ingester := IngesterDesc{
		Addr:      addr,
		Timestamp: time.Now().Unix(),
		State:     state,
	}

	if normaliseTokens {
		ingester.Tokens = tokens
	} else {
		for _, token := range tokens {
			d.Tokens = append(d.Tokens, TokenDesc{
				Token:    token,
				Ingester: id,
			})
		}
		sort.Sort(ByToken(d.Tokens))
	}

	d.Ingesters[id] = ingester
}

// RemoveIngester removes the given ingester and all its tokens.
func (d *Desc) RemoveIngester(id string) {
	delete(d.Ingesters, id)
	output := []TokenDesc{}
	for i := 0; i < len(d.Tokens); i++ {
		if d.Tokens[i].Ingester != id {
			output = append(output, d.Tokens[i])
		}
	}
	d.Tokens = output
}

// ClaimTokens transfers all the tokens from one ingester to another,
// returning the claimed token.
func (d *Desc) ClaimTokens(from, to string, normaliseTokens bool) []uint32 {
	var result []uint32

	// If the ingester we are claiming from is normalising, blank out its tokens.
	if fromDesc, found := d.Ingesters[from]; found {
		fromDesc.Tokens = nil
		d.Ingesters[from] = fromDesc
	}

	// Update the denormalised ring, and get our new tokens from there
	// In the normalised case we could take them from fromDesc.Tokens,
	// but we need this code anyway for the migrate-from-denormalised case.
	for i := 0; i < len(d.Tokens); i++ {
		if d.Tokens[i].Ingester == from {
			d.Tokens[i].Ingester = to
			result = append(result, d.Tokens[i].Token)
		}
	}

	// Update our normalised tokens
	if normaliseTokens {
		sort.Sort(uint32s(result))
		ing := d.Ingesters[to]
		ing.Tokens = result
		d.Ingesters[to] = ing
	}

	return result
}

// FindIngestersByState returns the list of ingesters in the given state
func (d *Desc) FindIngestersByState(state IngesterState) []IngesterDesc {
	var result []IngesterDesc
	for _, ing := range d.Ingesters {
		if ing.State == state {
			result = append(result, ing)
		}
	}
	return result
}

// Ready is true when all ingesters are active and healthy.
func (d *Desc) Ready(heartbeatTimeout time.Duration) bool {
	numTokens := len(d.Tokens)
	for _, ingester := range d.Ingesters {
		if time.Now().Sub(time.Unix(ingester.Timestamp, 0)) > heartbeatTimeout {
			return false
		} else if ingester.State != ACTIVE {
			return false
		}
		numTokens += len(ingester.Tokens)
	}

	return numTokens > 0
}

// TokensFor partitions the tokens into those for the given ID, and those for others.
func (d *Desc) TokensFor(id string) (tokens, other []uint32) {
	var takenTokens, myTokens []uint32
	for _, token := range d.Tokens {
		takenTokens = append(takenTokens, token.Token)
		if token.Ingester == id {
			myTokens = append(myTokens, token.Token)
		}
	}
	return myTokens, takenTokens
}
