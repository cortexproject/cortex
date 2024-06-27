package storegateway

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/store"

	"github.com/cortexproject/cortex/pkg/util"
)

func TestLimiter(t *testing.T) {
	l := &limiter{
		limiter: store.NewLimiter(2, prometheus.NewCounter(prometheus.CounterOpts{})),
	}

	assert.NoError(t, l.Reserve(1))
	assert.NoError(t, l.ReserveWithType(1, store.PostingsFetched))
	assert.Error(t, l.Reserve(1))
	assert.Error(t, l.ReserveWithType(1, store.PostingsFetched))
}

func TestCompositeLimiter(t *testing.T) {
	l := &compositeLimiter{
		limiters: []store.BytesLimiter{
			store.NewLimiter(2, prometheus.NewCounter(prometheus.CounterOpts{})),
			store.NewLimiter(1, prometheus.NewCounter(prometheus.CounterOpts{})),
		},
	}

	assert.NoError(t, l.ReserveWithType(1, store.PostingsFetched))
	assert.ErrorContains(t, l.ReserveWithType(1, store.PostingsFetched), "(422)")
}

func TestNewTokenBucketBytesLimiter(t *testing.T) {
	instanceTokenBucket := util.NewTokenBucket(3, 3, nil)
	userTokenBucket := util.NewTokenBucket(2, 2, nil)
	requestTokenBucket := util.NewTokenBucket(1, 1, nil)
	l := newTokenBucketBytesLimiter(instanceTokenBucket, userTokenBucket, requestTokenBucket, false, func(tokens uint64, dataType store.StoreDataType) int64 {
		if dataType == store.SeriesFetched {
			return int64(tokens) * 5
		}
		return int64(tokens)
	})

	// should force retrieve tokens from all buckets upon succeeding
	assert.NoError(t, l.ReserveWithType(2, store.PostingsFetched))
	assert.Equal(t, int64(1), instanceTokenBucket.RemainingTokens())
	assert.Equal(t, int64(0), userTokenBucket.RemainingTokens())
	assert.Equal(t, int64(-1), requestTokenBucket.RemainingTokens())

	// should fail if user token bucket is running low
	instanceTokenBucket.Refund(2)
	userTokenBucket.Refund(2)
	requestTokenBucket.Refund(2)
	assert.ErrorContains(t, l.ReserveWithType(3, store.PostingsFetched), "not enough tokens in user token bucket")
	assert.Equal(t, int64(3), instanceTokenBucket.RemainingTokens())
	assert.Equal(t, int64(2), userTokenBucket.RemainingTokens())
	assert.Equal(t, int64(1), requestTokenBucket.RemainingTokens())

	// should fail if pod token bucket is running low
	instanceTokenBucket.ForceRetrieve(2)
	assert.ErrorContains(t, l.ReserveWithType(2, store.PostingsFetched), "not enough tokens in pod token bucket")
	assert.Equal(t, int64(1), instanceTokenBucket.RemainingTokens())
	assert.Equal(t, int64(2), userTokenBucket.RemainingTokens())
	assert.Equal(t, int64(1), requestTokenBucket.RemainingTokens())

	// should retrieve different amount of tokens based on data type
	instanceTokenBucket.Refund(2)
	assert.ErrorContains(t, l.ReserveWithType(1, store.SeriesFetched), "not enough tokens in user token bucket")
	assert.Equal(t, int64(3), instanceTokenBucket.RemainingTokens())
	assert.Equal(t, int64(2), userTokenBucket.RemainingTokens())
	assert.Equal(t, int64(1), requestTokenBucket.RemainingTokens())

	// should always succeed if retrieve token bucket has enough tokens, although shared buckets are empty
	instanceTokenBucket.ForceRetrieve(3)
	userTokenBucket.ForceRetrieve(2)
	assert.NoError(t, l.ReserveWithType(1, store.PostingsFetched))
	assert.Equal(t, int64(-1), instanceTokenBucket.RemainingTokens())
	assert.Equal(t, int64(-1), userTokenBucket.RemainingTokens())
	assert.Equal(t, int64(0), requestTokenBucket.RemainingTokens())
}

func TestNewTokenBucketLimter_DryRun(t *testing.T) {
	instanceTokenBucket := util.NewTokenBucket(3, 3, nil)
	userTokenBucket := util.NewTokenBucket(2, 2, nil)
	requestTokenBucket := util.NewTokenBucket(1, 1, nil)
	l := newTokenBucketBytesLimiter(instanceTokenBucket, userTokenBucket, requestTokenBucket, true, func(tokens uint64, dataType store.StoreDataType) int64 {
		if dataType == store.SeriesFetched {
			return int64(tokens) * 5
		}
		return int64(tokens)
	})

	// should force retrieve tokens from all buckets upon succeeding
	assert.NoError(t, l.ReserveWithType(2, store.PostingsFetched))
	assert.False(t, instanceTokenBucket.Retrieve(2))
	assert.Equal(t, int64(1), instanceTokenBucket.RemainingTokens())
	assert.Equal(t, int64(0), userTokenBucket.RemainingTokens())
	assert.Equal(t, int64(-1), requestTokenBucket.RemainingTokens())

	// should not fail even if tokens are not enough
	instanceTokenBucket.Refund(2)
	userTokenBucket.Refund(2)
	requestTokenBucket.Refund(2)
	assert.NoError(t, l.ReserveWithType(5, store.PostingsFetched))
	assert.Equal(t, int64(-2), instanceTokenBucket.RemainingTokens())
	assert.Equal(t, int64(-3), userTokenBucket.RemainingTokens())
	assert.Equal(t, int64(-4), requestTokenBucket.RemainingTokens())
}
