package distributed_execution

import (
	"encoding/json"
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/logicalplan"
)

type NodeType = logicalplan.NodeType
type Node = logicalplan.Node

const (
	RemoteNode = "RemoteNode"
)

// (to verify interface implementations)
var _ logicalplan.Node = (*Remote)(nil)

type Remote struct {
	Op   parser.ItemType
	Expr Node `json:"-"`

	FragmentKey  FragmentKey
	FragmentAddr string
}

func NewRemoteNode() Node {
	return &Remote{
		// initialize the fragment key pointer first
		FragmentKey: FragmentKey{},
	}
}
func (r *Remote) Clone() Node {
	return &Remote{Op: r.Op, Expr: r.Expr.Clone(), FragmentKey: r.FragmentKey, FragmentAddr: r.FragmentAddr}
}
func (r *Remote) Children() []*Node {
	return []*Node{&r.Expr}
}
func (r *Remote) String() string {
	return fmt.Sprintf("%s%s", r.Op.String(), r.Expr.String())
}
func (r *Remote) ReturnType() parser.ValueType {
	return r.Expr.ReturnType()
}
func (r *Remote) Type() NodeType { return RemoteNode }

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

	r.FragmentKey = *MakeFragmentKey(re.QueryID, re.FragmentID)
	r.FragmentAddr = re.FragmentAddr
	return nil
}
