package encoding

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Encoding defines which encoding we are using, delta, doubledelta, or varbit
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

const (
	// PrometheusXorChunk is a wrapper around Prometheus XOR-encoded chunk.
	// 4 is the magic value for backwards-compatibility with previous iota-based constants.
	PrometheusXorChunk Encoding = 4
)

type encoding struct {
	Name     string
	Encoding chunkenc.Encoding
}

var encodings = map[Encoding]encoding{
	PrometheusXorChunk: {
		Name:     "PrometheusXorChunk",
		Encoding: chunkenc.EncXOR,
	},
}

func FromPromChunkEncoding(enc chunkenc.Encoding) (Encoding, error) {
	switch enc {
	case chunkenc.EncXOR:
		return PrometheusXorChunk, nil
	}
	return Encoding(0), errors.Errorf("unknown Prometheus chunk encoding: %v", enc)
}
