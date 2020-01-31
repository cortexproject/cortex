package main

import (
	"testing"

	proto12 "github.com/cortexproject/cortex/integration-tests/proto/1.2.1"
	proto13 "github.com/cortexproject/cortex/integration-tests/proto/1.3.0"
	"github.com/stretchr/testify/require"
)

func Test_CustomMarshal(t *testing.T) {
	l13 := proto13.LabelAdapter{Name: "foo", Value: "bar"}
	b3, err := l13.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	l12 := proto12.LabelAdapter{}
	err = l12.Unmarshal(b3)
	if err != nil {
		t.Fatal(err)
	}
	require.EqualValues(t, l13, l12)

	b2, err := l12.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	err = l13.Unmarshal(b2)
	if err != nil {
		t.Fatal(err)
	}
	require.EqualValues(t, l13, l12)

	t13 := proto13.TimeSeries{
		Labels: []proto13.LabelAdapter{
			proto13.LabelAdapter{Name: "1", Value: "2"},
		},
		Samples: []proto13.Sample{
			proto13.Sample{Value: 10, TimestampMs: 1},
			proto13.Sample{Value: 10000, TimestampMs: 2},
		},
	}
	bt13, err := t13.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	t12 := proto12.TimeSeries{}
	err = t12.Unmarshal(bt13)
	if err != nil {
		t.Fatal(err)
	}
	require.EqualValues(t, t13, t12)

	bt12, err := t12.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	t13 = proto13.TimeSeries{}
	err = t13.Unmarshal(bt12)
	if err != nil {
		t.Fatal(err)
	}
	require.EqualValues(t, t13, t12)
}
