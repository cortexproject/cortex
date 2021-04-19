package encoding

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
)

// Encoding defines which encoding we are using, delta, doubledelta, or varbit
type Encoding byte

// Config configures the behaviour of chunk encoding
type Config struct {
	EncodingName string `yaml:"chunk_encoding"`
}

var (
	// DefaultEncoding exported for use in unit tests elsewhere
	DefaultEncoding      = Bigchunk
	bigchunkSizeCapBytes = 0
)

// RegisterFlags registers configuration settings.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.EncodingName, "encoding.chunk-encoding", "big-chunk", "Encoding version to use for chunks.")
	f.IntVar(&bigchunkSizeCapBytes, "store.bigchunk-size-cap-bytes", bigchunkSizeCapBytes, "When using bigchunk encoding, start a new bigchunk if over this size (0 = unlimited)")
}

// Validate errors out if the encoding is set to Delta.
func (Config) Validate() error {
	if DefaultEncoding == Delta {
		// Delta is deprecated.
		return errors.New("delta encoding is deprecated")
	}
	return nil
}

// String implements flag.Value.
func (e Encoding) String() string {
	if known, found := encodings[e]; found {
		return known.Name
	}
	return fmt.Sprintf("%d", e)
}

const (
	// Delta encoding is no longer supported and will be automatically changed to DoubleDelta.
	// It still exists here to not change the `ingester.chunk-encoding` flag values.
	Delta Encoding = iota
	// DoubleDelta encoding
	DoubleDelta
	// Varbit encoding
	Varbit
	// Bigchunk encoding
	Bigchunk
	// PrometheusXorChunk is a wrapper around Prometheus XOR-encoded chunk.
	PrometheusXorChunk
)

type encoding struct {
	Name string
	New  func() Chunk
}

var encodings = map[Encoding]encoding{
	DoubleDelta: {
		Name: "double-delta",
		New: func() Chunk {
			return newDoubleDeltaEncodedChunk(d1, d0, true, ChunkLen)
		},
	},
	Varbit: {
		Name: "varbit",
		New: func() Chunk {
			return newVarbitChunk(varbitZeroEncoding)
		},
	},
	Bigchunk: {
		Name: "big-chunk",
		New: func() Chunk {
			return newBigchunk()
		},
	},
	PrometheusXorChunk: {
		Name: "prometheus-xor-chunk",
		New: func() Chunk {
			return newPrometheusXorChunk()
		},
	},
}

// Set implements flag.Value.
func (e *Encoding) Set(s string) error {
	// If nothing is provided, keep the original value
	if s == "" {
		return nil
	}

	// Then see if the name was given
	for k, v := range encodings {
		if s == v.Name {
			*e = k
			return nil
		}
	}
	// Otherwise, accept a number
	i, err := strconv.Atoi(s)
	if err != nil {
		return err
	}

	_, ok := encodings[Encoding(i)]
	if !ok {
		return fmt.Errorf("invalid chunk encoding: %s", s)
	}

	*e = Encoding(i)
	return nil
}

// New creates a new chunk according to the encoding set by the
// DefaultEncoding flag.
func New() Chunk {
	chunk, err := NewForEncoding(DefaultEncoding)
	if err != nil {
		panic(err)
	}
	return chunk
}

// NewForEncoding allows configuring what chunk type you want
func NewForEncoding(encoding Encoding) (Chunk, error) {
	enc, ok := encodings[encoding]
	if !ok {
		return nil, fmt.Errorf("unknown chunk encoding: %v", encoding)
	}

	return enc.New(), nil
}

// MustRegisterEncoding add a new chunk encoding.  There is no locking, so this
// must be called in init().
func MustRegisterEncoding(enc Encoding, name string, f func() Chunk) {
	_, ok := encodings[enc]
	if ok {
		panic("double register encoding")
	}

	encodings[enc] = encoding{
		Name: name,
		New:  f,
	}
}
