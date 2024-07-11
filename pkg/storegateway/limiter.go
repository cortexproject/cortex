package storegateway

import (
	"fmt"
	"net/http"
	"sync"

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
			return httpgrpc.Errorf(http.StatusUnprocessableEntity, err.Error())
		}
	}
	return nil
}

type tokenBucketBytesLimiter struct {
	instanceTokenBucket *util.TokenBucket
	userTokenBucket     *util.TokenBucket
	requestTokenBucket  *util.TokenBucket
	getTokensToRetrieve func(tokens uint64, dataType store.StoreDataType) int64
	dryRun              bool

	// Counter metric which we will increase if limit is exceeded.
	failedCounter prometheus.Counter
	failedOnce    sync.Once
}

func (t *tokenBucketBytesLimiter) Reserve(_ uint64) error {
	return nil
}

func (t *tokenBucketBytesLimiter) ReserveWithType(num uint64, dataType store.StoreDataType) error {
	tokensToRetrieve := t.getTokensToRetrieve(num, dataType)

	requestTokenRemaining := t.requestTokenBucket.Retrieve(tokensToRetrieve)
	userTokenRemaining := t.userTokenBucket.Retrieve(tokensToRetrieve)
	instanceTokenRemaining := t.instanceTokenBucket.Retrieve(tokensToRetrieve)

	// if we can retrieve from request bucket, let the request go through
	if requestTokenRemaining >= 0 {
		return nil
	}

	errMsg := ""

	if userTokenRemaining < 0 {
		errMsg = "not enough tokens in user token bucket"
	} else if instanceTokenRemaining < 0 {
		errMsg = "not enough tokens in instance token bucket"
	}

	if errMsg != "" {
		if t.dryRun {
			level.Warn(util_log.Logger).Log("msg", errMsg, "dataType", dataType, "dataSize", num, "tokens", tokensToRetrieve)
			return nil
		}

		// We need to protect from the counter being incremented twice due to concurrency
		t.failedOnce.Do(t.failedCounter.Inc)
		return fmt.Errorf(errMsg)
	}

	return nil
}

func newTokenBucketBytesLimiter(requestTokenBucket, userTokenBucket, instanceTokenBucket *util.TokenBucket, dryRun bool, failedCounter prometheus.Counter, getTokensToRetrieve func(tokens uint64, dataType store.StoreDataType) int64) *tokenBucketBytesLimiter {
	return &tokenBucketBytesLimiter{
		requestTokenBucket:  requestTokenBucket,
		userTokenBucket:     userTokenBucket,
		instanceTokenBucket: instanceTokenBucket,
		dryRun:              dryRun,
		failedCounter:       failedCounter,
		getTokensToRetrieve: getTokensToRetrieve,
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

func newBytesLimiterFactory(limits *validation.Overrides, userID string, userTokenBucket, instanceTokenBucket *util.TokenBucket, tokenBucketBytesLimiterCfg tsdb.TokenBucketBytesLimiterConfig, getTokensToRetrieve func(tokens uint64, dataType store.StoreDataType) int64) store.BytesLimiterFactory {
	return func(failedCounter prometheus.Counter) store.BytesLimiter {
		limiters := []store.BytesLimiter{}
		// Since limit overrides could be live reloaded, we have to get the current user's limit
		// each time a new limiter is instantiated.
		limiters = append(limiters, store.NewLimiter(uint64(limits.MaxDownloadedBytesPerRequest(userID)), failedCounter))

		if tokenBucketBytesLimiterCfg.Enabled {
			requestTokenBucket := util.NewTokenBucket(tokenBucketBytesLimiterCfg.RequestTokenBucketSize, nil)
			limiters = append(limiters, newTokenBucketBytesLimiter(requestTokenBucket, userTokenBucket, instanceTokenBucket, tokenBucketBytesLimiterCfg.DryRun, failedCounter, getTokensToRetrieve))
		}

		return &compositeLimiter{
			limiters: limiters,
		}
	}
}
