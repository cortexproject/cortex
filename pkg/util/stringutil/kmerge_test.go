package stringutil

import (
	"reflect"
	"testing"
)

func Test_kWayMerge(t *testing.T) {
	tests := []struct {
		name   string
		arrays [][]string
		want   []string
	}{
		{
			name: "test",
			arrays: [][]string{
				{"a", "c", "e"},
				{"c", "d", "f"},
				{"c", "f"},
				{"k", "m"},
				{"b"},
				{"a"},
			},
			want: []string{"a", "b", "c", "d", "e", "f", "k", "m"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := KWayMerge(tt.arrays...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("kWayMerge() = %v, want %v", got, tt.want)
			}
		})
	}
}
