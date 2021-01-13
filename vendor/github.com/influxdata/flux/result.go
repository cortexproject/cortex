package flux

import (
	"io"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux/iocounter"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

type Result interface {
	Name() string
	// Tables returns a TableIterator for iterating through results
	Tables() TableIterator
}

type TableIterator interface {
	Do(f func(Table) error) error
}

// Table represents a set of streamed data with a common schema.
// The contents of the table can be read exactly once.
//
// This data structure is not thread-safe.
type Table interface {
	// Key returns the set of data that is common among all rows
	// in the table.
	Key() GroupKey

	// Cols contains metadata about the column schema.
	Cols() []ColMeta

	// Do calls f to process the data contained within the table.
	// This must only be called once and implementations should return
	// an error if this is called multiple times.
	Do(f func(ColReader) error) error

	// Done indicates that this table is no longer needed and that the
	// underlying processor that produces the table may discard any
	// buffers that need to be processed. If the table has already been
	// read with Do, this happens automatically.
	// This is also not required if the table is empty.
	// It should be safe to always call this function and call it multiple
	// times.
	Done()

	// Empty returns whether the table contains no records.
	// This should not return true when the table is empty because of an error.
	Empty() bool
}

// BufferedTable is an implementation of Table that has all of its
// data buffered.
type BufferedTable interface {
	Table

	// Buffer returns the i'th buffer in the buffered table.
	// This allows accessing the buffered table contents without
	// using the Table.
	Buffer(i int) ColReader

	// BufferN returns the number of buffers in this table.
	BufferN() int

	// Copy will return a copy of the BufferedTable without
	// consuming the Table itself. If this Table has already
	// been consumed by the Do method, then this will panic.
	Copy() BufferedTable
}

// ColMeta contains the information about the column metadata.
type ColMeta struct {
	// Label is the name of the column. The label is unique per table.
	Label string
	// Type is the type of the column. Only basic types are allowed.
	Type ColType
}

// ColType is the type for a column. This covers only basic data types.
type ColType int

const (
	TInvalid ColType = iota
	TBool
	TInt
	TUInt
	TFloat
	TString
	TTime
)

// ColumnType returns the column type when given a semantic.Type.
// It returns flux.TInvalid if the Type is not a valid column type.
func ColumnType(typ semantic.MonoType) ColType {
	switch typ.Nature() {
	case semantic.Bool:
		return TBool
	case semantic.Int:
		return TInt
	case semantic.UInt:
		return TUInt
	case semantic.Float:
		return TFloat
	case semantic.String:
		return TString
	case semantic.Time:
		return TTime
	default:
		return TInvalid
	}
}

func SemanticType(typ ColType) semantic.MonoType {
	switch typ {
	case TBool:
		return semantic.BasicBool
	case TInt:
		return semantic.BasicInt
	case TUInt:
		return semantic.BasicUint
	case TFloat:
		return semantic.BasicFloat
	case TString:
		return semantic.BasicString
	case TTime:
		return semantic.BasicTime
	default:
		return semantic.MonoType{}
	}
}

// String returns a string representation of the column type.
func (t ColType) String() string {
	switch t {
	case TInvalid:
		return "invalid"
	case TBool:
		return "bool"
	case TInt:
		return "int"
	case TUInt:
		return "uint"
	case TFloat:
		return "float"
	case TString:
		return "string"
	case TTime:
		return "time"
	default:
		return "unknown"
	}
}

// ColReader allows access to reading arrow buffers of column data.
// All data the ColReader exposes is guaranteed to be in memory.
// A ColReader that is produced when processing a Table will be
// released once it goes out of scope. Retain can be used to keep
// a reference to the buffered memory.
type ColReader interface {
	Key() GroupKey
	// Cols returns a list of column metadata.
	Cols() []ColMeta
	// Len returns the length of the slices.
	// All slices will have the same length.
	Len() int
	Bools(j int) *array.Boolean
	Ints(j int) *array.Int64
	UInts(j int) *array.Uint64
	Floats(j int) *array.Float64
	Strings(j int) *array.Binary
	Times(j int) *array.Int64

	// Retain will retain this buffer to avoid having the
	// memory consumed by it freed.
	Retain()

	// Release will release a reference to this buffer.
	Release()
}

type GroupKey interface {
	Cols() []ColMeta
	Values() []values.Value

	HasCol(label string) bool
	LabelValue(label string) values.Value

	IsNull(j int) bool
	ValueBool(j int) bool
	ValueUInt(j int) uint64
	ValueInt(j int) int64
	ValueFloat(j int) float64
	ValueString(j int) string
	ValueDuration(j int) values.Duration
	ValueTime(j int) values.Time
	Value(j int) values.Value

	Equal(o GroupKey) bool
	Less(o GroupKey) bool
	String() string
}

// GroupKeys provides a sortable collection of group keys.
type GroupKeys []GroupKey

func (a GroupKeys) Len() int           { return len(a) }
func (a GroupKeys) Less(i, j int) bool { return a[i].Less(a[j]) }
func (a GroupKeys) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// String returns a string representation of the keys
func (a GroupKeys) String() string {
	var result string
	for _, key := range a {
		result += key.String() + "\n"
	}
	return result
}

// ResultDecoder can decode a result from a reader.
type ResultDecoder interface {
	// Decode decodes data from r into a result.
	Decode(r io.Reader) (Result, error)
}

// ResultEncoder can encode a result into a writer.
type ResultEncoder interface {
	// Encode encodes data from the result into w.
	// Returns the number of bytes written to w and any error.
	Encode(w io.Writer, result Result) (int64, error)
}

// MultiResultDecoder can decode multiple results from a reader.
type MultiResultDecoder interface {
	// Decode decodes multiple results from r.
	Decode(r io.ReadCloser) (ResultIterator, error)
}

// MultiResultEncoder can encode multiple results into a writer.
type MultiResultEncoder interface {
	// Encode writes multiple results from r into w.
	// Returns the number of bytes written to w and any error resulting from the encoding process.
	// It is up to the specific implementation for whether it will encode any errors that occur
	// from the ResultIterator.
	Encode(w io.Writer, results ResultIterator) (int64, error)
}

// EncoderError is an interface that any error produced from
// a ResultEncoder implementation should conform to.
// It allows for differentiation
// between errors that occur in results, and errors that occur while encoding results.
type EncoderError interface {
	IsEncoderError() bool
}

// isEncoderError reports whether or not the underlying cause of
// an error is a valid EncoderError.
func isEncoderError(err error) bool {
	encErr, ok := err.(EncoderError)
	return ok && encErr.IsEncoderError()
}

// DelimitedMultiResultEncoder encodes multiple results using a trailing delimiter.
// The delimiter is written after every result.
//
// If an error is encountered when iterating and the error is an encoder error,
// the error will be returned. Otherwise, the error is assumed to
// have arisen from query execution, and said error will be encoded with the
// EncodeError method of the Encoder field.
//
// If the io.Writer implements flusher, it will be flushed after each delimiter.
type DelimitedMultiResultEncoder struct {
	Delimiter []byte
	Encoder   interface {
		ResultEncoder
		// EncodeError encodes an error on the writer.
		EncodeError(w io.Writer, err error) error
	}
}

type flusher interface {
	Flush()
}

// Encode will encode the results into the writer using the Encoder and separating each entry
// by the Delimiter. If an error occurs while processing the ResultIterator or is returned from
// the underlying Encoder, Encode will return the error if nothing has yet been written to the
// Writer. If something has been written to the Writer, then an error will only be returned
// when the error is an EncoderError.
func (e *DelimitedMultiResultEncoder) Encode(w io.Writer, results ResultIterator) (int64, error) {
	wc := &iocounter.Writer{Writer: w}

	for results.More() {
		result := results.Next()
		if _, err := e.Encoder.Encode(wc, result); err != nil {
			// If we have an error that's from encoding or if we have not
			// yet written any data to the writer, return the error.
			if isEncoderError(err) || wc.Count() == 0 {
				return wc.Count(), err
			}
			// Otherwise, the error happened during query execution and we
			// are stuck encoding it.
			err := e.Encoder.EncodeError(wc, err)
			return wc.Count(), err
		}
		if _, err := wc.Write(e.Delimiter); err != nil {
			return wc.Count(), err
		}
		// Flush the writer after each result.
		if f, ok := w.(flusher); ok {
			f.Flush()
		}
	}

	// If we have any outlying errors in results, encode them
	// If we have an error in the result and we have not written
	// to the writer, then return the error as-is. Otherwise, encode
	// it the same way we do above.
	if err := results.Err(); err != nil {
		if wc.Count() == 0 {
			return 0, err
		}
		err := e.Encoder.EncodeError(wc, err)
		return wc.Count(), err
	}
	return wc.Count(), nil
}
