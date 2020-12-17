package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsTenantDir(t *testing.T) {
	assert.False(t, isTenantBlocksDir(""))
	assert.True(t, isTenantBlocksDir("test"))
	assert.True(t, isTenantBlocksDir("test/"))
	assert.False(t, isTenantBlocksDir("test/block"))
	assert.False(t, isTenantBlocksDir("test/block/chunks"))
}

func TestIsBucketIndexFile(t *testing.T) {
	assert.False(t, isTenantBlocksDir(""))
	assert.False(t, isTenantBlocksDir("test"))
	assert.False(t, isTenantBlocksDir("test/block"))
	assert.False(t, isTenantBlocksDir("test/block/chunks"))
	assert.True(t, isTenantBlocksDir("test/bucket-index.json.gz"))
}
