package bucketindex

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func TestBlockDeletionMarkFilepath(t *testing.T) {
	id := ulid.MustNew(1, nil)

	assert.Equal(t, "markers/"+id.String()+"-deletion-mark.json", BlockDeletionMarkFilepath(id))
}

func TestIsBlockDeletionMarkFilename(t *testing.T) {
	expected := ulid.MustNew(1, nil)

	_, ok := IsBlockDeletionMarkFilename("xxx")
	assert.False(t, ok)

	_, ok = IsBlockDeletionMarkFilename("xxx-deletion-mark.json")
	assert.False(t, ok)

	_, ok = IsBlockDeletionMarkFilename("tenant-deletion-mark.json")
	assert.False(t, ok)

	actual, ok := IsBlockDeletionMarkFilename(expected.String() + "-deletion-mark.json")
	assert.True(t, ok)
	assert.Equal(t, expected, actual)
}
