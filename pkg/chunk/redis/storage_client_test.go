package redis

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/testutils"
)

func TestNewFailsWhenNoRedisURL(t *testing.T) {
	cfg := Config{}
	_, err := NewRedisObjectClient(cfg, chunk.SchemaConfig{})
	require.Error(t, err)
}

func TestNewCreatesMemoryServer(t *testing.T) {
	cfg := Config{
		Redis: []string{"inmemory:///"},
	}

	s, err := NewRedisObjectClient(cfg, chunk.SchemaConfig{})
	require.NoError(t, err)
	defer s.Stop()

	_, chunks, err := testutils.CreateChunks(0, 1, model.Now())
	require.NoError(t, err)
	err = s.PutChunks(context.TODO(), chunks)
	require.NoError(t, err)

	got, err := s.GetChunks(context.TODO(), chunks)
	require.NoError(t, err)
	require.Len(t, got, 1)

	exp := chunks
	require.Equal(t, exp, got)
}

func TestPutChunkDoesNotSetTTLWhenNoRetentionIsConfigured(t *testing.T) {
	cfg := Config{
		Redis: []string{"inmemory:///"},
	}

	s, err := NewRedisObjectClient(cfg, chunk.SchemaConfig{})
	require.NoError(t, err)
	defer s.Stop()

	_, chunks, err := testutils.CreateChunks(0, 1, model.Now())
	require.NoError(t, err)
	err = s.PutChunks(context.TODO(), chunks)
	require.NoError(t, err)

	got := s.memoryServer.TTL(chunks[0].ExternalKey())
	exp := time.Duration(0)
	require.Equal(t, exp, got)
}

func TestPutChunkSetsTTLWhenRetentionIsConfigured(t *testing.T) {
	cfg := Config{
		Redis:          []string{"inmemory:///"},
		ChunkRetention: time.Hour,
	}

	s, err := NewRedisObjectClient(cfg, chunk.SchemaConfig{})
	require.NoError(t, err)
	defer s.Stop()

	_, chunks, err := testutils.CreateChunks(0, 1, model.Now().Add(-time.Minute))
	require.NoError(t, err)
	err = s.PutChunks(context.TODO(), chunks)
	require.NoError(t, err)

	ttl := s.memoryServer.TTL(chunks[0].ExternalKey())
	require.True(t, ttl > 0, "expected ttl to be gt 0, but got %d", ttl)

	// fast forward the memory server to expire the keys
	s.memoryServer.FastForward(cfg.ChunkRetention + time.Second)

	got, err := s.GetChunks(context.TODO(), chunks)
	require.NoError(t, err)
	require.Equal(t, []chunk.Chunk{{}}, got)
}

func TestPutChunkSetsTTLCorrectly(t *testing.T) {
	cfg := Config{
		Redis:          []string{"inmemory:///"},
		ChunkRetention: time.Hour,
	}
	mnow := model.Now()
	nowFunc = func() model.Time {
		return mnow
	}

	s, err := NewRedisObjectClient(cfg, chunk.SchemaConfig{})
	require.NoError(t, err)
	defer s.Stop()

	_, chunks, err := testutils.CreateChunks(0, 1, mnow)
	require.NoError(t, err)
	err = s.PutChunks(context.TODO(), chunks)
	require.NoError(t, err)

	require.EqualValues(t, mnow, chunks[0].Through)

	exp := cfg.ChunkRetention
	got := s.memoryServer.TTL(chunks[0].ExternalKey())
	require.EqualValues(t, exp, got)
}
