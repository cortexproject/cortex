package chunk

import (
	"reflect"
	"testing"
)

func c(id string) Chunk {
	return Chunk{UserID: id}
}

func TestUnique(t *testing.T) {
	for _, tc := range []struct {
		in   ByKey
		want ByKey
	}{
		{nil, ByKey{}},
		{ByKey{c("a"), c("a")}, ByKey{c("a")}},
		{ByKey{c("a"), c("a"), c("b"), c("b"), c("c")}, ByKey{c("a"), c("b"), c("c")}},
		{ByKey{c("a"), c("b"), c("c")}, ByKey{c("a"), c("b"), c("c")}},
	} {
		have := unique(tc.in)
		if !reflect.DeepEqual(have, tc.want) {
			t.Errorf("%v != %v", have, tc.want)
		}
	}
}

func TestMerge(t *testing.T) {
	type args struct {
		a ByKey
		b ByKey
	}
	for _, tc := range []struct {
		args args
		want ByKey
	}{
		{args{ByKey{}, ByKey{}}, ByKey{}},
		{args{ByKey{c("a")}, ByKey{}}, ByKey{c("a")}},
		{args{ByKey{}, ByKey{c("b")}}, ByKey{c("b")}},
		{args{ByKey{c("a")}, ByKey{c("b")}}, ByKey{c("a"), c("b")}},
		{
			args{ByKey{c("a"), c("c")}, ByKey{c("a"), c("b"), c("d")}},
			ByKey{c("a"), c("b"), c("c"), c("d")}},
	} {
		have := merge(tc.args.a, tc.args.b)
		if !reflect.DeepEqual(have, tc.want) {
			t.Errorf("%v != %v", have, tc.want)
		}
	}
}

func TestNWayUnion(t *testing.T) {
	for _, tc := range []struct {
		in   []ByKey
		want ByKey
	}{
		{nil, ByKey{}},
		{[]ByKey{{c("a")}}, ByKey{c("a")}},
		{[]ByKey{{c("a")}, {}}, ByKey{c("a")}},
		{[]ByKey{{}, {c("b")}}, ByKey{c("b")}},
		{[]ByKey{{c("a")}, {c("b")}}, ByKey{c("a"), c("b")}},
		{
			[]ByKey{{c("a"), c("c"), c("e")}, {c("c"), c("d")}, {c("b")}},
			ByKey{c("a"), c("b"), c("c"), c("d"), c("e")},
		},
	} {
		have := nWayUnion(tc.in)
		if !reflect.DeepEqual(have, tc.want) {
			t.Errorf("%v != %v", have, tc.want)
		}
	}
}

func TestNWayIntersect(t *testing.T) {
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
