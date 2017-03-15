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
		in   []ByKey
		want ByKey
	}{
		{nil, ByKey{}},
		{[]ByKey{{c("a"), c("b"), c("c")}}, []Chunk{c("a"), c("b"), c("c")}},
		{[]ByKey{{c("a"), c("b"), c("c")}, {c("a"), c("c")}}, ByKey{c("a"), c("c")}},
		{[]ByKey{{c("a"), c("b"), c("c")}, {c("a"), c("c")}, {c("b")}}, ByKey{}},
		{[]ByKey{{c("a"), c("b"), c("c")}, {c("a"), c("c")}, {c("a")}}, ByKey{c("a")}},
	} {
		have := nWayIntersect(tc.in)
		if !reflect.DeepEqual(have, tc.want) {
			t.Errorf("%v != %v", have, tc.want)
		}
	}
}
