package storegateway

import (
	"fmt"
	"net/http"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/weaveworks/common/httpgrpc"
)

type limiter struct {
	limiter *store.Limiter
}

func (c *limiter) Reserve(num uint64) error {
	return c.ReserveWithType(num, 0)
}

func (c *limiter) ReserveWithType(num uint64, _ store.StoreDataType) error {
	err := c.limiter.Reserve(num)
	if err != nil {
		return httpgrpc.Errorf(http.StatusUnprocessableEntity, err.Error())
	}

	return nil
}

type compositeLimiter struct {
	limiters []store.BytesLimiter
}

func (c *compositeLimiter) ReserveWithType(num uint64, dataType store.StoreDataType) error {
	for _, l := range c.limiters {
		if err := l.ReserveWithType(num, dataType); err != nil {
			return err
		}
	}
	return nil
}

type tokenBucketLimiter struct {
	podTokenBucket      *util.TokenBucket
	userTokenBucket     *util.TokenBucket
	requestTokenBucket  *util.TokenBucket
	getTokensToRetrieve func(tokens uint64, dataType store.StoreDataType) int64
	dryRun              bool
}

func (t *tokenBucketLimiter) Reserve(_ uint64) error {
	return nil
}

func (t *tokenBucketLimiter) ReserveWithType(num uint64, dataType store.StoreDataType) error {
	tokensToRetrieve := t.getTokensToRetrieve(num, dataType)

	// check request bucket
	retrieved := t.requestTokenBucket.Retrieve(tokensToRetrieve)
	if retrieved {
		t.userTokenBucket.ForceRetrieve(tokensToRetrieve)
		t.podTokenBucket.ForceRetrieve(tokensToRetrieve)
		return nil
	}

	// if request bucket is running low, check shared buckets
	retrieved = t.userTokenBucket.Retrieve(tokensToRetrieve)
	if !retrieved {
		return fmt.Errorf("not enough tokens in user token bucket")
	}

	retrieved = t.podTokenBucket.Retrieve(tokensToRetrieve)
	if !retrieved {
		t.userTokenBucket.Refund(tokensToRetrieve)
		return fmt.Errorf("not enough tokens in pod token bucket")
	}

	return nil
}

func newTokenBucketLimiter(podTokenBucket, userTokenBucket, requestTokenBucket *util.TokenBucket, dryRun bool, getTokensToRetrieve func(tokens uint64, dataType store.StoreDataType) int64) *tokenBucketLimiter {
	return &tokenBucketLimiter{
		podTokenBucket:      podTokenBucket,
		userTokenBucket:     userTokenBucket,
		requestTokenBucket:  requestTokenBucket,
		getTokensToRetrieve: getTokensToRetrieve,
		dryRun:              dryRun,
	}
}

func newChunksLimiterFactory(limits *validation.Overrides, userID string) store.ChunksLimiterFactory {
	return func(failedCounter prometheus.Counter) store.ChunksLimiter {
		// Since limit overrides could be live reloaded, we have to get the current user's limit
		// each time a new limiter is instantiated.
		return &limiter{
			limiter: store.NewLimiter(uint64(limits.MaxChunksPerQueryFromStore(userID)), failedCounter),
		}
	}
}

func newSeriesLimiterFactory(limits *validation.Overrides, userID string) store.SeriesLimiterFactory {
	return func(failedCounter prometheus.Counter) store.SeriesLimiter {
		// Since limit overrides could be live reloaded, we have to get the current user's limit
		// each time a new limiter is instantiated.
		return &limiter{
			limiter: store.NewLimiter(uint64(limits.MaxFetchedSeriesPerQuery(userID)), failedCounter),
		}
	}
}

func newBytesLimiterFactory(limits *validation.Overrides, userID string, podTokenBucket, userTokenBucket *util.TokenBucket, tokenBucketLimiterCfg tsdb.TokenBucketLimiterConfig, getTokensToRetrieve func(tokens uint64, dataType store.StoreDataType) int64) store.BytesLimiterFactory {
	return func(failedCounter prometheus.Counter) store.BytesLimiter {
		limiters := []store.BytesLimiter{}
		// Since limit overrides could be live reloaded, we have to get the current user's limit
		// each time a new limiter is instantiated.
		limiters = append(limiters, store.NewLimiter(uint64(limits.MaxDownloadedBytesPerRequest(userID)), failedCounter))

		if tokenBucketLimiterCfg.Enabled {
			requestTokenBucket := util.NewTokenBucket(tokenBucketLimiterCfg.RequestTokenBucketSize, tokenBucketLimiterCfg.RequestTokenBucketSize, nil)
			limiters = append(limiters, newTokenBucketLimiter(podTokenBucket, userTokenBucket, requestTokenBucket, tokenBucketLimiterCfg.DryRun, getTokensToRetrieve))
		}

		return &compositeLimiter{
			limiters: limiters,
		}
	}
}
