package storegateway

import (
	"fmt"
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/validation"
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
	instanceTokenBucket *util.TokenBucket
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
		t.instanceTokenBucket.ForceRetrieve(tokensToRetrieve)
		return nil
	}

	// if we can't retrieve from request bucket, check shared buckets
	retrieved = t.userTokenBucket.Retrieve(tokensToRetrieve)
	if !retrieved {
		// if dry run, force retrieve all tokens and return nil
		if t.dryRun {
			t.requestTokenBucket.ForceRetrieve(tokensToRetrieve)
			t.userTokenBucket.ForceRetrieve(tokensToRetrieve)
			t.instanceTokenBucket.ForceRetrieve(tokensToRetrieve)
			level.Warn(util_log.Logger).Log("msg", "not enough tokens in user token bucket", "dataType", dataType, "dataSize", num, "tokens", tokensToRetrieve)
			return nil
		}
		return fmt.Errorf("not enough tokens in user token bucket")
	}

	retrieved = t.instanceTokenBucket.Retrieve(tokensToRetrieve)
	if !retrieved {
		t.userTokenBucket.Refund(tokensToRetrieve)

		// if dry run, force retrieve all tokens and return nil
		if t.dryRun {
			// user bucket is already retrieved
			t.requestTokenBucket.ForceRetrieve(tokensToRetrieve)
			t.instanceTokenBucket.ForceRetrieve(tokensToRetrieve)
			level.Warn(util_log.Logger).Log("msg", "not enough tokens in pod token bucket", "dataType", dataType, "dataSize", num, "tokens", tokensToRetrieve)
			return nil
		}
		return fmt.Errorf("not enough tokens in pod token bucket")
	}

	t.requestTokenBucket.ForceRetrieve(tokensToRetrieve)
	return nil
}

func newTokenBucketLimiter(instanceTokenBucket, userTokenBucket, requestTokenBucket *util.TokenBucket, dryRun bool, getTokensToRetrieve func(tokens uint64, dataType store.StoreDataType) int64) *tokenBucketLimiter {
	return &tokenBucketLimiter{
		instanceTokenBucket: instanceTokenBucket,
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

func newBytesLimiterFactory(limits *validation.Overrides, userID string, instanceTokenBucket, userTokenBucket *util.TokenBucket, tokenBucketLimiterCfg tsdb.TokenBucketLimiterConfig, getTokensToRetrieve func(tokens uint64, dataType store.StoreDataType) int64) store.BytesLimiterFactory {
	return func(failedCounter prometheus.Counter) store.BytesLimiter {
		limiters := []store.BytesLimiter{}
		// Since limit overrides could be live reloaded, we have to get the current user's limit
		// each time a new limiter is instantiated.
		limiters = append(limiters, store.NewLimiter(uint64(limits.MaxDownloadedBytesPerRequest(userID)), failedCounter))

		if tokenBucketLimiterCfg.Enabled {
			requestTokenBucket := util.NewTokenBucket(tokenBucketLimiterCfg.RequestTokenBucketSize, tokenBucketLimiterCfg.RequestTokenBucketSize, nil)
			limiters = append(limiters, newTokenBucketLimiter(instanceTokenBucket, userTokenBucket, requestTokenBucket, tokenBucketLimiterCfg.DryRun, getTokensToRetrieve))
		}

		return &compositeLimiter{
			limiters: limiters,
		}
	}
}
