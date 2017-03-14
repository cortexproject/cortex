package chunk

import (
	"reflect"
	"testing"
)

func c(id string) Chunk {
	return Chunk{UserID: id}
}

func TestIntersect(t *testing.T) {
	for _, tc := range []struct {
		in   []ByID
		want ByID
	}{
		{nil, ByID{}},
		{[]ByID{{c("a"), c("b"), c("c")}}, []Chunk{c("a"), c("b"), c("c")}},
		{[]ByID{{c("a"), c("b"), c("c")}, {c("a"), c("c")}}, ByID{c("a"), c("c")}},
		{[]ByID{{c("a"), c("b"), c("c")}, {c("a"), c("c")}, {c("b")}}, ByID{}},
		{[]ByID{{c("a"), c("b"), c("c")}, {c("a"), c("c")}, {c("a")}}, ByID{c("a")}},
	} {
		have := nWayIntersect(tc.in)
		if !reflect.DeepEqual(have, tc.want) {
			t.Errorf("%v != %v", have, tc.want)
		}
	}
}
