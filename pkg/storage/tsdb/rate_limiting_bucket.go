package tsdb

import (
	"context"
	"errors"
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/time/rate"
)

// ErrRateLimited is returned by the Iter() method when it is rate limited
var ErrRateLimited = errors.New("too many requests")

// Check that the RateLimitingBucket implements the objstore.Bucket interface
var _ objstore.Bucket = &RateLimitingBucket{}

var (
	rateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_bucket_rate_limited_total",
		Help: "The total number of times a method of the bucket was rate limited.",
	}, []string{"method"})
)

// RateLimitingBucket is a objstore.Bucket that rate limits calls
// to the Iter() method
type RateLimitingBucket struct {
	bkt     objstore.Bucket
	limiter *rate.Limiter
}

// NewRateLimitingBucket creates a new RateLimitingBucket
func NewRateLimitingBucket(bkt objstore.Bucket, limit rate.Limit, burst int) RateLimitingBucket {
	return RateLimitingBucket{
		bkt:     bkt,
		limiter: rate.NewLimiter(limit, burst),
	}
}

// Iter calls the same method in the underlying bucket after checking the rate limiter.
func (rlb RateLimitingBucket) Iter(ctx context.Context, dir string, f func(string) error) (err error) {
	if rlb.limiter.Allow() == false {
		rateLimited.WithLabelValues("iter").Inc()
		return ErrRateLimited
	}

	return rlb.bkt.Iter(ctx, dir, f)
}

// Get calls the same method in the underlying bucket.
func (rlb RateLimitingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return rlb.bkt.Get(ctx, name)
}

// GetRange calls the same method in the underlying bucket.
func (rlb RateLimitingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return rlb.bkt.GetRange(ctx, name, off, length)
}

// Exists calls the same method in the underlying bucket.
func (rlb RateLimitingBucket) Exists(ctx context.Context, name string) (exists bool, err error) {
	return rlb.bkt.Exists(ctx, name)
}

// Attributes calls the same method in the underlying bucket.
func (rlb RateLimitingBucket) Attributes(ctx context.Context, name string) (attrs objstore.ObjectAttributes, err error) {
	return rlb.bkt.Attributes(ctx, name)
}

// IsObjNotFoundErr calls the same method in the underlying bucket.
func (rlb RateLimitingBucket) IsObjNotFoundErr(err error) bool {
	return rlb.bkt.IsObjNotFoundErr(err)
}

// Upload calls the same method in the underlying bucket.
func (rlb RateLimitingBucket) Upload(ctx context.Context, name string, r io.Reader) (err error) {
	return rlb.bkt.Upload(ctx, name, r)
}

// Delete calls the same method in the underlying bucket.
func (rlb RateLimitingBucket) Delete(ctx context.Context, name string) (err error) {
	return rlb.bkt.Delete(ctx, name)
}

// Name calls the same method in the underlying bucket.
func (rlb RateLimitingBucket) Name() string {
	return rlb.bkt.Name()
}

// Close calls the same method in the underlying bucket.
func (rlb RateLimitingBucket) Close() error {
	return rlb.bkt.Close()
}
