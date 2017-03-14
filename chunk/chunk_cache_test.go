package chunk

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

type mockMemcache struct {
	sync.Mutex
	contents map[string]*memcache.Item
}

func newMockMemcache() *mockMemcache {
	return &mockMemcache{
		contents: map[string]*memcache.Item{},
	}
}

func (m *mockMemcache) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	m.Lock()
	defer m.Unlock()
	result := map[string]*memcache.Item{}
	for _, k := range keys {
		if c, ok := m.contents[k]; ok {
			result[k] = c
		}
	}
	return result, nil
}

func (m *mockMemcache) Set(item *memcache.Item) error {
	m.Lock()
	defer m.Unlock()
	m.contents[item.Key] = item
	return nil
}

func TestChunkCache(t *testing.T) {
	c := Cache{
		memcache: newMockMemcache(),
	}

	const (
		chunkLen = 13 * 3600 // in seconds
		userID   = "1"
	)

	// put 100 chunks from 0 to 99
	ids := []string{}
	chunks := []Chunk{}
	for i := 0; i < 100; i++ {
		ts := model.TimeFromUnix(int64(i * chunkLen))
		promChunk, _ := chunk.New().Add(model.SamplePair{Timestamp: ts, Value: 0})
		chunk := NewChunk(
			model.Fingerprint(1),
			model.Metric{
				model.MetricNameLabel: "foo",
				"bar": "baz",
			},
			promChunk[0],
			ts,
			ts.Add(chunkLen),
		)
		ids = append(ids, chunk.ID)
		chunks = append(chunks, chunk)
	}

	err := c.StoreChunks(context.Background(), userID, chunks)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		id := ids[rand.Intn(len(ids))]
		found, missing, err := c.FetchChunkData(context.Background(), userID, []Chunk{{ID: id}})
		assert.NoError(t, err)
		assert.Empty(t, missing)
		assert.Len(t, found, 1)
		assert.Equal(t, id, found[0].ID)
	}
}
