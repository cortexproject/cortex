package distributed_execution

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"
)

func TestRemoteNode(t *testing.T) {
	t.Run("NewRemoteNode creates valid node", func(t *testing.T) {
		node := &Remote{}
		require.NotNil(t, node)
		require.IsType(t, &Remote{}, node)
		require.Equal(t, (&Remote{}).Type(), node.Type())
	})

	t.Run("Clone creates correct copy", func(t *testing.T) {
		original := &Remote{
			FragmentKey:  FragmentKey{queryID: 1, fragmentID: 2},
			FragmentAddr: "[IP_ADDRESS]:9090",
			Expr:         &logicalplan.NumberLiteral{Val: 42},
		}

		cloned := original.Clone()
		require.NotNil(t, cloned)

		remote, ok := cloned.(*Remote)
		require.True(t, ok)
		require.Equal(t, original.FragmentKey, remote.FragmentKey)
		require.Equal(t, original.FragmentAddr, remote.FragmentAddr)
		require.Equal(t, original.Expr.String(), remote.Expr.String())
	})

	t.Run("Children returns correct nodes", func(t *testing.T) {
		expr := &logicalplan.NumberLiteral{Val: 42}
		node := &Remote{
			Expr: expr,
		}

		children := node.Children()
		require.Len(t, children, 1)
		require.Equal(t, expr, *children[0])
	})

	t.Run("ReturnType matches expression type", func(t *testing.T) {
		expr := &logicalplan.NumberLiteral{Val: 42}
		node := &Remote{
			Expr: expr,
		}

		require.Equal(t, expr.ReturnType(), node.ReturnType())
	})
}
