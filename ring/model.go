package ring

import (
	"sort"
	"time"

	"github.com/prometheus/common/log"
)

// IngesterState describes the state of an ingester
type IngesterState int

// Values for IngesterState
const (
	Active IngesterState = iota
	Leaving
)

func (s IngesterState) String() string {
	switch s {
	case Active:
		return "Active"
	case Leaving:
		return "Leaving"
	}
	return ""
}

// Desc is the serialised state in Consul representing
// all ingesters (ie, the ring).
type Desc struct {
	Ingesters map[string]IngesterDesc `json:"ingesters"`
	Tokens    TokenDescs              `json:"tokens"`
}

// IngesterDesc describes a single ingester.
type IngesterDesc struct {
	Hostname  string        `json:"hostname"`
	Timestamp time.Time     `json:"timestamp"`
	State     IngesterState `json:"state"`
}

// TokenDescs is a sortable list of TokenDescs
type TokenDescs []TokenDesc

func (ts TokenDescs) Len() int           { return len(ts) }
func (ts TokenDescs) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts TokenDescs) Less(i, j int) bool { return ts[i].Token < ts[j].Token }

// TokenDesc describes an individual token in the ring.
type TokenDesc struct {
	Token    uint32 `json:"tokens"`
	Ingester string `json:"ingester"`
}

func descFactory() interface{} {
	return newDesc()
}

func newDesc() *Desc {
	return &Desc{
		Ingesters: map[string]IngesterDesc{},
	}
}

func (d *Desc) addIngester(id, hostname string, tokens []uint32, state IngesterState) {
	d.Ingesters[id] = IngesterDesc{
		Hostname:  hostname,
		Timestamp: time.Now(),
		State:     state,
	}

	for _, token := range tokens {
		d.Tokens = append(d.Tokens, TokenDesc{
			Token:    token,
			Ingester: id,
		})
	}

	sort.Sort(d.Tokens)
}

func (d *Desc) removeIngester(id string, tokens []uint32) {
	delete(d.Ingesters, id)
	output := []TokenDesc{}
	i, j := 0, 0
	for i < len(d.Tokens) && j < len(tokens) {
		if d.Tokens[i].Token < tokens[j] {
			output = append(output, d.Tokens[i])
			i++
		} else if d.Tokens[i].Token > tokens[j] {
			log.Infof("Missing token from ring: %d", tokens[j])
			j++
		} else {
			i++
			j++
		}
	}
	for i < len(d.Tokens) {
		output = append(output, d.Tokens[i])
		i++
	}
	for j < len(tokens) {
		log.Infof("Missing token from ring: %d", tokens[j])
		j++
	}
	d.Tokens = output
}
