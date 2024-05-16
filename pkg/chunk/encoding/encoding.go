package encoding

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Encoding defines which encoding we are using.
type Encoding byte

// String implements flag.Value.
func (e Encoding) String() string {
	if known, found := encodings[e]; found {
		return known.Name
	}
	return fmt.Sprintf("%d", e)
}

func (e Encoding) PromChunkEncoding() chunkenc.Encoding {
	if known, found := encodings[e]; found {
		return known.Encoding
	}
	return chunkenc.EncNone
}

func (e Encoding) ChunkValueType() chunkenc.ValueType {
	if known, found := encodings[e]; found {
		return known.ValueType
	}
	return chunkenc.ValNone
}

const (
	// PrometheusXorChunk is a wrapper around Prometheus XOR-encoded chunk.
	// 4 is the magic value for backwards-compatibility with previous iota-based constants.
	PrometheusXorChunk Encoding = 4
	// PrometheusHistogramChunk is a wrapper around Prometheus histogram chunk.
	// 5 is the magic value for backwards-compatibility with previous iota-based constants.
	PrometheusHistogramChunk Encoding = 5
	// PrometheusFloatHistogramChunk is a wrapper around Prometheus float histogram chunk.
	// 6 is the magic value for backwards-compatibility with previous iota-based constants.
	PrometheusFloatHistogramChunk Encoding = 6
)

type encoding struct {
	Name      string
	Encoding  chunkenc.Encoding
	ValueType chunkenc.ValueType
}

var encodings = map[Encoding]encoding{
	PrometheusXorChunk: {
		Name:      "PrometheusXorChunk",
		Encoding:  chunkenc.EncXOR,
		ValueType: chunkenc.ValFloat,
	},
	PrometheusHistogramChunk: {
		Name:      "PrometheusHistogramChunk",
		Encoding:  chunkenc.EncHistogram,
		ValueType: chunkenc.ValHistogram,
	},
	PrometheusFloatHistogramChunk: {
		Name:      "PrometheusFloatHistogramChunk",
		Encoding:  chunkenc.EncFloatHistogram,
		ValueType: chunkenc.ValFloatHistogram,
	},
}

func FromPromChunkEncoding(enc chunkenc.Encoding) (Encoding, error) {
	switch enc {
	case chunkenc.EncXOR:
		return PrometheusXorChunk, nil
	case chunkenc.EncHistogram:
		return PrometheusHistogramChunk, nil
	case chunkenc.EncFloatHistogram:
		return PrometheusFloatHistogramChunk, nil
	}
	return Encoding(0), errors.Errorf("unknown Prometheus chunk encoding: %v", enc)
}
