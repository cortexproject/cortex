package chunk

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/docker/docker/pkg/ioutils"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	prom_chunk "github.com/prometheus/prometheus/storage/local/chunk"

	"github.com/weaveworks/cortex/util"
)

// Chunk contains encoded timeseries data
type Chunk struct {
	ID      string       `json:"-"`
	From    model.Time   `json:"from"`
	Through model.Time   `json:"through"`
	Metric  model.Metric `json:"metric"`

	// We never use Delta encoding (the zero value), so if this entry is
	// missing, we default to DoubleDelta.
	Encoding prom_chunk.Encoding `json:"encoding"`
	Data     prom_chunk.Chunk    `json:"-"`

	// Flag indicates if metadata was written to index, and if false implies
	// we should read a header of the chunk containing the metadata.  Exists
	// for backwards compatibility with older chunks, which did not have header.
	metadataInIndex bool
}

// ByID allow you to sort chunks by ID
type ByID []Chunk

func (cs ByID) Len() int           { return len(cs) }
func (cs ByID) Swap(i, j int)      { cs[i], cs[j] = cs[j], cs[i] }
func (cs ByID) Less(i, j int) bool { return cs[i].ID < cs[j].ID }

// NewChunk creates a new chunk
func NewChunk(fp model.Fingerprint, metric model.Metric, c *prom_chunk.Desc) Chunk {
	return Chunk{
		ID:       fmt.Sprintf("%d:%d:%d", fp, c.ChunkFirstTime, c.ChunkLastTime),
		From:     c.ChunkFirstTime,
		Through:  c.ChunkLastTime,
		Metric:   metric,
		Encoding: c.C.Encoding(),
		Data:     c.C,
	}
}

func (c *Chunk) reader() (io.ReadSeeker, error) {
	// Encode chunk metadata into snappy-compressed buffer
	var metadata bytes.Buffer
	if err := json.NewEncoder(snappy.NewWriter(&metadata)).Encode(c); err != nil {
		return nil, err
	}

	lenBytes := [4]byte{}
	binary.BigEndian.PutUint32(lenBytes[:], uint32(metadata.Len()))

	// TODO consider adding a .Reader() to upstream to remove copy
	data := make([]byte, prom_chunk.ChunkLen)
	if err := c.Data.MarshalToBuf(data); err != nil {
		return nil, err
	}

	// Body is chunk bytes (uncompressed) with metadata appended on the end.
	return ioutils.MultiReadSeeker(
		bytes.NewReader(lenBytes[:]),
		bytes.NewReader(metadata.Bytes()),
		bytes.NewReader(data),
	), nil
}

func (c *Chunk) decode(r io.Reader) error {
	// Legacy chunks were written with metadata in the index.
	if c.metadataInIndex {
		var err error
		c.Data, err = prom_chunk.NewForEncoding(prom_chunk.DoubleDelta)
		if err != nil {
			return err
		}
		return c.Data.Unmarshal(r)
	}

	var metadataLen uint32
	if err := binary.Read(r, binary.BigEndian, &metadataLen); err != nil {
		return err
	}

	err := json.NewDecoder(snappy.NewReader(&io.LimitedReader{
		N: int64(metadataLen),
		R: r,
	})).Decode(c)
	if err != nil {
		return err
	}

	if c.Encoding == prom_chunk.Delta {
		c.Encoding = prom_chunk.DoubleDelta
	}

	c.Data, err = prom_chunk.NewForEncoding(c.Encoding)
	if err != nil {
		return err
	}

	return c.Data.Unmarshal(r)
}

// ChunksToMatrix converts a slice of chunks into a model.Matrix.
func ChunksToMatrix(chunks []Chunk) (model.Matrix, error) {
	// Group chunks by series, sort and dedupe samples.
	sampleStreams := map[model.Fingerprint]*model.SampleStream{}
	for _, c := range chunks {
		fp := c.Metric.Fingerprint()
		ss, ok := sampleStreams[fp]
		if !ok {
			ss = &model.SampleStream{
				Metric: c.Metric,
			}
			sampleStreams[fp] = ss
		}

		samples, err := c.samples()
		if err != nil {
			return nil, err
		}

		ss.Values = util.MergeSamples(ss.Values, samples)
	}

	matrix := make(model.Matrix, 0, len(sampleStreams))
	for _, ss := range sampleStreams {
		matrix = append(matrix, ss)
	}

	return matrix, nil
}

func (c *Chunk) samples() ([]model.SamplePair, error) {
	it := c.Data.NewIterator()
	// TODO(juliusv): Pre-allocate this with the right length again once we
	// add a method upstream to get the number of samples in a chunk.
	var samples []model.SamplePair
	for it.Scan() {
		samples = append(samples, it.Value())
	}
	return samples, nil
}
