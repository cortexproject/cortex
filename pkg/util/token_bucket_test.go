package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTokenBucket_Retrieve(t *testing.T) {
	bucket := NewTokenBucket(10, 600, nil)

	assert.True(t, bucket.Retrieve(10))
	assert.False(t, bucket.Retrieve(10))
	time.Sleep(time.Second)
	assert.True(t, bucket.Retrieve(10))
	assert.Equal(t, int64(0), bucket.RemainingTokens())
}

func TestTokenBucket_ForceRetrieve(t *testing.T) {
	bucket := NewTokenBucket(10, 600, nil)

	bucket.ForceRetrieve(20)
	assert.Equal(t, int64(-10), bucket.RemainingTokens())
	assert.False(t, bucket.Retrieve(10))
	time.Sleep(time.Second)
	assert.True(t, bucket.Retrieve(10))
	assert.Equal(t, int64(0), bucket.RemainingTokens())
}

func TestTokenBucket_Refund(t *testing.T) {
	bucket := NewTokenBucket(10, 600, nil)

	bucket.ForceRetrieve(10)
	bucket.Refund(20)
	assert.True(t, bucket.Retrieve(10))
	assert.Equal(t, int64(0), bucket.RemainingTokens())
}
