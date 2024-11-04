package storegateway

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestChunkBytesPool_Get(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewPedanticRegistry()
	p, err := newChunkBytesPool(cortex_tsdb.ChunkPoolDefaultMinBucketSize, cortex_tsdb.ChunkPoolDefaultMaxBucketSize, 0, reg)
	require.NoError(t, err)
	testBytes := []byte("test")
	b0, err := p.Get(store.EstimatedMaxChunkSize - 1)
	require.NoError(t, err)

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(fmt.Sprintf(`
		# HELP cortex_bucket_store_chunk_pool_inuse_bytes Total bytes in use in the chunk pool.
		# TYPE cortex_bucket_store_chunk_pool_inuse_bytes gauge
        cortex_bucket_store_chunk_pool_inuse_bytes %d
	`, 16000)), "cortex_bucket_store_chunk_pool_inuse_bytes"))

	b, err := p.Get(store.EstimatedMaxChunkSize + 1)
	require.NoError(t, err)

	*b = append(*b, testBytes...)

	p.Put(b)

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(fmt.Sprintf(`
		# HELP cortex_bucket_store_chunk_pool_inuse_bytes Total bytes in use in the chunk pool.
		# TYPE cortex_bucket_store_chunk_pool_inuse_bytes gauge
        cortex_bucket_store_chunk_pool_inuse_bytes %d
		# HELP cortex_bucket_store_chunk_pool_operation_bytes_total Total bytes number of bytes pooled by operation.
		# TYPE cortex_bucket_store_chunk_pool_operation_bytes_total counter
		cortex_bucket_store_chunk_pool_operation_bytes_total{operation="get",stats="cap"} %d
		cortex_bucket_store_chunk_pool_operation_bytes_total{operation="get",stats="requested"} %d
		cortex_bucket_store_chunk_pool_operation_bytes_total{operation="put",stats="cap"} %d
		cortex_bucket_store_chunk_pool_operation_bytes_total{operation="put",stats="len"} %d
	`, 16000, store.EstimatedMaxChunkSize*3, store.EstimatedMaxChunkSize*2, store.EstimatedMaxChunkSize*2, len(testBytes)))))

	p.Put(b0)
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(fmt.Sprintf(`
		# HELP cortex_bucket_store_chunk_pool_inuse_bytes Total bytes in use in the chunk pool.
		# TYPE cortex_bucket_store_chunk_pool_inuse_bytes gauge
        cortex_bucket_store_chunk_pool_inuse_bytes %d
	`, 0)), "cortex_bucket_store_chunk_pool_inuse_bytes"))
}
