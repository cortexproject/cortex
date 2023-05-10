package iterators

import (
	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

var ErrNativeHistogramsUnsupported = errors.Newf("querying native histograms is not supported")

// compChunksIterator is a compatible interface from prometheus before the native histograms implementation.
// As for now cortex does not support this feature, this interface was create as a bridge and should be deleted
// together with the  compatibleChunksIterator when cortex supports native histograms.
// See: https://github.com/prometheus/prometheus/blob/5ddfba78936b7ca7ed3600869f623359e6b5ecb0/tsdb/chunkenc/chunk.go#L89-L105
type compChunksIterator interface {
	Next() bool
	Seek(t int64) bool
	At() (int64, float64)
	Err() error
}

type compatibleChunksIterator struct {
	chunkenc.Iterator
	it  compChunksIterator
	err error
}

//revive:disable:unexported-return
func NewCompatibleChunksIterator(i compChunksIterator) *compatibleChunksIterator {
	return &compatibleChunksIterator{it: i}
}

//revive:enable:unexported-return

func (c *compatibleChunksIterator) Next() chunkenc.ValueType {
	if c.it.Next() {
		return chunkenc.ValFloat
	}

	return chunkenc.ValNone
}

func (c *compatibleChunksIterator) Seek(t int64) chunkenc.ValueType {
	if c.it.Seek(t) {
		return chunkenc.ValFloat
	}

	return chunkenc.ValNone
}

func (c *compatibleChunksIterator) At() (int64, float64) {
	return c.it.At()
}

func (c *compatibleChunksIterator) AtHistogram() (int64, *histogram.Histogram) {
	c.err = ErrNativeHistogramsUnsupported
	return 0, nil
}

func (c *compatibleChunksIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	c.err = ErrNativeHistogramsUnsupported
	return 0, nil
}

func (c *compatibleChunksIterator) AtT() int64 {
	t, _ := c.it.At()
	return t
}

func (c *compatibleChunksIterator) Err() error {
	if c.err != nil {
		return c.err
	}

	return c.it.Err()
}

func (c *compatibleChunksIterator) AtTime() int64 {
	return c.AtT()
}
