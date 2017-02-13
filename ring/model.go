package ring

import (
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
)

// ByToken is a sortable list of TokenDescs
type ByToken []*TokenDesc

func (ts ByToken) Len() int           { return len(ts) }
func (ts ByToken) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts ByToken) Less(i, j int) bool { return ts[i].Token < ts[j].Token }

// ProtoDescFactory makes new Descs
func ProtoDescFactory() proto.Message {
	return newDesc()
}

func newDesc() *Desc {
	return &Desc{
		Ingesters: map[string]*IngesterDesc{},
	}
}

func (d *Desc) addIngester(id, addr string, tokens []uint32, state IngesterState) {
	if d.Ingesters == nil {
		d.Ingesters = map[string]*IngesterDesc{}
	}
	d.Ingesters[id] = &IngesterDesc{
		Addr:      addr,
		Timestamp: time.Now().Unix(),
		State:     state,
		ProtoRing: true,
	}

	for _, token := range tokens {
		d.Tokens = append(d.Tokens, &TokenDesc{
			Token:    token,
			Ingester: id,
		})
	}

	sort.Sort(ByToken(d.Tokens))
}

func (d *Desc) removeIngester(id string) {
	delete(d.Ingesters, id)
	output := []*TokenDesc{}
	for i := 0; i < len(d.Tokens); i++ {
		if d.Tokens[i].Ingester != id {
			output = append(output, d.Tokens[i])
		}
	}
	d.Tokens = output
}
