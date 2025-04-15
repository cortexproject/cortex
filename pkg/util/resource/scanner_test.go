package resource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NoopScanner(t *testing.T) {
	s := noopScanner{}
	val, err := s.scan()
	require.NoError(t, err)
	require.Zero(t, val)
}
