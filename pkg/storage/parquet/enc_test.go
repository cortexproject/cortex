package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeUsintSlicesSlices(t *testing.T) {
	slices := [][]uint64{
		{},
		{1, 2, 3},
	}

	for _, slice := range slices {
		t.Run(fmt.Sprint(slice), func(t *testing.T) {
			encode := EncodeUintSlice(slice)
			decode, err := DecodeUintSlice(encode)
			require.NoError(t, err)
			require.Equal(t, slice, decode)
		})
	}

}

func TestEncodeDecodeStringSlicesSlices(t *testing.T) {
	slices := [][]string{
		{},
		{"", "0", "foo", "bar"},
	}

	for _, slice := range slices {
		t.Run(fmt.Sprint(slice), func(t *testing.T) {
			encode := EncodeStringSlice(slice)
			decode, err := DecodeStringSlice(encode)
			require.NoError(t, err)
			require.Equal(t, slice, decode)
		})
	}

}
