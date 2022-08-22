package encoding

import (
	"fmt"
)

// Encoding defines which encoding we are using, delta, doubledelta, or varbit
type Encoding byte

var (
	// DefaultEncoding exported for use in unit tests elsewhere
	DefaultEncoding = PrometheusXorChunk
)

// String implements flag.Value.
func (e Encoding) String() string {
	if known, found := encodings[e]; found {
		return known.Name
	}
	return fmt.Sprintf("%d", e)
}

const (
	// PrometheusXorChunk is a wrapper around Prometheus XOR-encoded chunk.
	// 4 is the magic value for backwards-compatibility with previous iota-based constants.
	PrometheusXorChunk Encoding = 4
)

type encoding struct {
	Name string
	New  func() Chunk
}

var encodings = map[Encoding]encoding{
	PrometheusXorChunk: {
		Name: "PrometheusXorChunk",
		New: func() Chunk {
			return newPrometheusXorChunk()
		},
	},
}

// NewForEncoding allows configuring what chunk type you want
func NewForEncoding(encoding Encoding) (Chunk, error) {
	enc, ok := encodings[encoding]
	if !ok {
		return nil, fmt.Errorf("unknown chunk encoding: %v", encoding)
	}

	return enc.New(), nil
}
