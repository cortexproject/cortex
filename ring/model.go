package ring

import (
	"encoding/json"
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

// DescFactory makes new Descs
func DescFactory() interface{} {
	return newDesc()
}

func newDesc() *Desc {
	return &Desc{
		Ingesters: map[string]*IngesterDesc{},
	}
}

type oldIngesterDesc struct {
	Hostname     string        `json:"hostname"`
	Timestamp    time.Time     `json:"timestamp"`
	State        IngesterState `json:"state"`
	GRPCHostname string        `json:"grpc_hostname"`
	ProtoRing    bool          `json:"proto_ring"`
}

// UnmarshalJSON allows the new proto IngesterDescs to read the old JSON format.
//
// NB grpc_hostname in the old format is just hostname in the new.
func (d *IngesterDesc) UnmarshalJSON(in []byte) error {
	var tmp oldIngesterDesc
	if err := json.Unmarshal(in, &tmp); err != nil {
		return err
	}

	d.Hostname = tmp.GRPCHostname
	d.Timestamp = tmp.Timestamp.Unix()
	d.State = tmp.State
	d.ProtoRing = tmp.ProtoRing
	return nil
}

// MarshalJSON allows the new proto IngesterDescs to write the old JSON format.
//
// NB grpc_hostname in the old format is just hostname in the new.
func (d *IngesterDesc) MarshalJSON() ([]byte, error) {
	return json.Marshal(oldIngesterDesc{
		Hostname:     "",
		Timestamp:    time.Unix(d.Timestamp, 0),
		State:        d.State,
		GRPCHostname: d.Hostname,
		ProtoRing:    d.ProtoRing,
	})
}

func (d *Desc) addIngester(id, hostname string, tokens []uint32, state IngesterState) {
	if d.Ingesters == nil {
		d.Ingesters = map[string]*IngesterDesc{}
	}
	d.Ingesters[id] = &IngesterDesc{
		Hostname:  hostname,
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
