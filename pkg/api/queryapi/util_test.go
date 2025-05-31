package queryapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Convert(t *testing.T) {
	time := time.Now().UnixMilli()

	require.Equal(t, time, convertMsToTime(time).UnixMilli())
	require.Equal(t, time, convertMsToDuration(time).Milliseconds())
}
