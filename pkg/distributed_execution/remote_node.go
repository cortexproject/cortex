package distributed_execution

import (
	"encoding/json"
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/logicalplan"
)

const (
	RemoteNode = "RemoteNode"
)

// (to verify interface implementations)
var _ logicalplan.Node = (*Remote)(nil)

// Remote is a custom node that marks where the portion of logical plan
// that needs to be executed remotely
type Remote struct {
	Expr logicalplan.Node `json:"-"`

	FragmentKey  FragmentKey
	FragmentAddr string
}

func NewRemoteNode(Expr logicalplan.Node) logicalplan.Node {
	return &Remote{
		// initialize the fragment key pointer first
		Expr:        Expr,
		FragmentKey: FragmentKey{},
	}
}
func (r *Remote) Clone() logicalplan.Node {
	return &Remote{Expr: r.Expr.Clone(), FragmentKey: r.FragmentKey, FragmentAddr: r.FragmentAddr}
}
func (r *Remote) Children() []*logicalplan.Node {
	return []*logicalplan.Node{&r.Expr}
}
func (r *Remote) String() string {
	return fmt.Sprintf("remote(%s)", r.Expr.String())
}
func (r *Remote) ReturnType() parser.ValueType {
	return r.Expr.ReturnType()
}
func (r *Remote) Type() logicalplan.NodeType { return RemoteNode }

type remote struct {
	QueryID      uint64
	FragmentID   uint64
	FragmentAddr string
}

func (r *Remote) MarshalJSON() ([]byte, error) {
	return json.Marshal(remote{
		QueryID:      r.FragmentKey.queryID,
		FragmentID:   r.FragmentKey.fragmentID,
		FragmentAddr: r.FragmentAddr,
	})
}

func (r *Remote) UnmarshalJSON(data []byte) error {
	re := remote{}
	if err := json.Unmarshal(data, &re); err != nil {
		return err
	}

	r.FragmentKey = MakeFragmentKey(re.QueryID, re.FragmentID)
	r.FragmentAddr = re.FragmentAddr
	return nil
}
