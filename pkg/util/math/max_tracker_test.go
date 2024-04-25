package math

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxTracker(t *testing.T) {
	mt := MaxTracker{}
	mt.Track(50)
	require.Equal(t, int64(50), mt.Load())
	mt.Tick()
	require.Equal(t, int64(50), mt.Load())
	mt.Tick()
	require.Equal(t, int64(0), mt.Load())
}
