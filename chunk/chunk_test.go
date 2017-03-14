package chunk

import (
	//"bytes"
	//"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/stretchr/testify/assert"

	"github.com/weaveworks/common/test"
)

const userID = "userID"

func dummyChunk() Chunk {
	return dummyChunkFor(model.Metric{
		model.MetricNameLabel: "foo",
		"bar":  "baz",
		"toms": "code",
	})
}

func dummyChunkFor(metric model.Metric) Chunk {
	now := model.Now()
	cs, _ := chunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})
	chunk := NewChunk(
		userID,
		metric.Fingerprint(),
		metric,
		cs[0],
		now.Add(-time.Hour),
		now,
	)
	return chunk
}

func TestChunkCodec(t *testing.T) {
	want := dummyChunk()
	buf, err := want.encode()
	assert.NoError(t, err)

	have, err := parseExternalKey("", want.externalKey())
	assert.NoError(t, err)

	err = have.decode(buf)
	assert.NoError(t, err)

	if !reflect.DeepEqual(want, have) {
		t.Fatalf("wrong chunks - " + test.Diff(want, have))
	}
}
