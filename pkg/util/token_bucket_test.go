package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTokenBucket_Retrieve(t *testing.T) {
	bucket := NewTokenBucket(10, nil)

	assert.Equal(t, int64(0), bucket.Retrieve(10))
	assert.Negative(t, bucket.Retrieve(1))
	time.Sleep(time.Second)
	assert.Positive(t, bucket.Retrieve(5))
}

func TestTokenBucket_MaxCapacity(t *testing.T) {
	bucket := NewTokenBucket(10, nil)

	assert.Equal(t, int64(10), bucket.MaxCapacity())
}
