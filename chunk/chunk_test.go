package chunk

import (
	//"bytes"
	//"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"

	"github.com/weaveworks/common/test"
)

func TestChunkCodec(t *testing.T) {
	now := model.Now()
	cs, _ := chunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})
	want := Chunk{
		ID:      "",
		From:    now.Add(-time.Hour),
		Through: now,
		Metric: model.Metric{
			model.MetricNameLabel: "foo",
			"bar":  "baz",
			"toms": "code",
		},
		Encoding: chunk.DoubleDelta,
		Data:     cs[0],
	}

	r, err := want.reader()
	if err != nil {
		t.Fatalf("reader() error: %v", err)
	}

	have := Chunk{}
	if err := have.decode(r); err != nil {
		t.Fatalf("decode() error: %v", err)
	}

	if !reflect.DeepEqual(want, have) {
		t.Fatalf("wrong chunks - " + test.Diff(want, have))
	}
}
