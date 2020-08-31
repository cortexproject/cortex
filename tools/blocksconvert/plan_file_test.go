package blocksconvert

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsProgressFile(t *testing.T) {
	for _, tc := range []struct {
		input string
		exp   bool
		base  string
		t     time.Time
	}{
		{input: "hello/world.progress.123456", exp: true, base: "hello/world", t: time.Unix(123456, 0)},
		{input: "hello/world.progress.123456123456123456123456123456123456", exp: false, base: "", t: time.Time{}},
		{input: "hello/world.notprogress.123456", exp: false, base: "", t: time.Time{}},
		{input: "hello/world.plan", exp: false, base: "", t: time.Time{}},
	} {
		ok, base, tm := IsProgressFilename(tc.input)
		require.Equal(t, tc.exp, ok, tc.input)
		require.Equal(t, tc.base, base, tc.input)
		require.Equal(t, tc.t, tm, tc.input)
	}
}
