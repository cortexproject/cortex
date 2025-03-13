package storegateway

import (
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

const tokenBucketBytesLimiterErrStr = "store gateway resource exhausted"

type limiter struct {
	limiter *store.Limiter
}

func (c *limiter) Reserve(num uint64) error {
	return c.ReserveWithType(num, 0)
}

func (c *limiter) ReserveWithType(num uint64, _ store.StoreDataType) error {
	err := c.limiter.Reserve(num)
	if err != nil {
		return httpgrpc.Errorf(http.StatusUnprocessableEntity, "%s", err.Error())
	}

	return nil
}

type compositeBytesLimiter struct {
	limiters []store.BytesLimiter
}

func (c *compositeBytesLimiter) ReserveWithType(num uint64, dataType store.StoreDataType) error {
	for _, l := range c.limiters {
		if err := l.ReserveWithType(num, dataType); err != nil {
			return err // nested limiters are expected to return httpgrpc error
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

	errCode := 0

	if tokensToRetrieve > t.userTokenBucket.MaxCapacity() || tokensToRetrieve > t.instanceTokenBucket.MaxCapacity() {
		errCode = http.StatusUnprocessableEntity
	} else if userTokenRemaining < 0 || instanceTokenRemaining < 0 {
		errCode = http.StatusTooManyRequests
	}

	if errCode > 0 {
		if t.dryRun {
			level.Warn(util_log.Logger).Log("msg", tokenBucketBytesLimiterErrStr, "dataType", dataType, "dataSize", num, "tokens", tokensToRetrieve, "errorCode", errCode)
			return nil
		}

		// We need to protect from the counter being incremented twice due to concurrency
		t.failedOnce.Do(t.failedCounter.Inc)
		return httpgrpc.Errorf(errCode, tokenBucketBytesLimiterErrStr)
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
		limiters = append(limiters, &limiter{limiter: store.NewLimiter(uint64(limits.MaxDownloadedBytesPerRequest(userID)), failedCounter)})

		if tokenBucketBytesLimiterCfg.Mode != string(tsdb.TokenBucketBytesLimiterDisabled) {
			requestTokenBucket := util.NewTokenBucket(tokenBucketBytesLimiterCfg.RequestTokenBucketSize, nil)
			dryRun := tokenBucketBytesLimiterCfg.Mode == string(tsdb.TokenBucketBytesLimiterDryRun)
			limiters = append(limiters, newTokenBucketBytesLimiter(requestTokenBucket, userTokenBucket, instanceTokenBucket, dryRun, failedCounter, getTokensToRetrieve))
		}

		return &compositeBytesLimiter{
			limiters: limiters,
		}
	}
}
