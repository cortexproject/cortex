package querier

import (
	"github.com/prometheus/common/model"
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
	var (
		chunks = make([]chunk.Chunk, 0, numChunks)
		from   = model.Time(0)
	)
	for i := 0; i < numChunks; i++ {
		c := util.GenerateChunk(t, sampleRate, from, int(samplesPerChunk), enc)
		chunks = append(chunks, c)
		from = from.Add(chunkOffset)
	}
	return mockChunkStore{chunks}, from
}
