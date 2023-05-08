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
			},
			want: []string{"a", "c", "d", "e", "f"},
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
