package querier

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/util"
)

type mockChunkStore struct {
	chunks []chunk.Chunk
}

func (m mockChunkStore) Get() ([]chunk.Chunk, error) {
	return m.chunks, nil
}

func makeMockChunkStore(t require.TestingT, numChunks int, enc encoding.Encoding) (mockChunkStore, model.Time) {
	chks, from := makeMockChunks(t, numChunks, enc, 0)
	return mockChunkStore{chks}, from
}

func makeMockChunks(t require.TestingT, numChunks int, enc encoding.Encoding, from model.Time, additionalLabels ...labels.Label) ([]chunk.Chunk, model.Time) {
	var (
		chunks = make([]chunk.Chunk, 0, numChunks)
	)
	for i := 0; i < numChunks; i++ {
		c := util.GenerateChunk(t, sampleRate, from, int(samplesPerChunk), enc, additionalLabels...)
		chunks = append(chunks, c)
		from = from.Add(chunkOffset)
	}
	return chunks, from
}
