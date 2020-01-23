package tsdb

import (
	"context"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/objstore"
)

var (
	operations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_tsdb_objstore_bucket_operations_total",
		Help: "Total number of operations against a bucket.",
	}, []string{"component", "operation"})

	operationFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_tsdb_objstore_bucket_operation_failures_total",
		Help: "Total number of operations against a bucket that failed.",
	}, []string{"component", "operation"})

	operationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_tsdb_objstore_bucket_operation_duration_seconds",
		Help:    "Duration of operations against the bucket",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	}, []string{"component", "operation"})
)

// BucketWithMetrics takes a bucket and registers metrics with the given registry for
// operations run against the bucket.
func BucketWithMetrics(component string, b objstore.Bucket) objstore.Bucket {
	bkt := &metricBucket{
		bkt: b,

		ops:         operations.MustCurryWith(prometheus.Labels{"component": component}),
		opsFailures: operationFailures.MustCurryWith(prometheus.Labels{"component": component}),
		opsDuration: operationDuration.MustCurryWith(prometheus.Labels{"component": component}),
	}
	return bkt
}

type metricBucket struct {
	bkt objstore.Bucket

	ops         *prometheus.CounterVec
	opsFailures *prometheus.CounterVec
	opsDuration prometheus.ObserverVec
}

func (b *metricBucket) Iter(ctx context.Context, dir string, f func(name string) error) error {
	const op = "iter"

	err := b.bkt.Iter(ctx, dir, f)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()

	return err
}

// ObjectSize returns the size of the specified object.
func (b *metricBucket) ObjectSize(ctx context.Context, name string) (uint64, error) {
	const op = "objectsize"
	b.ops.WithLabelValues(op).Inc()
	start := time.Now()

	rc, err := b.bkt.ObjectSize(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
		return 0, err
	}
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	return rc, nil
}

func (b *metricBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	const op = "get"
	b.ops.WithLabelValues(op).Inc()

	rc, err := b.bkt.Get(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
		return nil, err
	}
	rc = newTimingReadCloser(
		rc,
		op,
		b.opsDuration,
		b.opsFailures,
	)

	return rc, nil
}

func (b *metricBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	const op = "get_range"
	b.ops.WithLabelValues(op).Inc()

	rc, err := b.bkt.GetRange(ctx, name, off, length)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
		return nil, err
	}
	rc = newTimingReadCloser(
		rc,
		op,
		b.opsDuration,
		b.opsFailures,
	)

	return rc, nil
}

func (b *metricBucket) Exists(ctx context.Context, name string) (bool, error) {
	const op = "exists"
	start := time.Now()

	ok, err := b.bkt.Exists(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return ok, err
}

func (b *metricBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	const op = "upload"
	start := time.Now()

	err := b.bkt.Upload(ctx, name, r)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return err
}

func (b *metricBucket) Delete(ctx context.Context, name string) error {
	const op = "delete"
	start := time.Now()

	err := b.bkt.Delete(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return err
}

func (b *metricBucket) IsObjNotFoundErr(err error) bool {
	return b.bkt.IsObjNotFoundErr(err)
}

func (b *metricBucket) Close() error {
	return b.bkt.Close()
}

func (b *metricBucket) Name() string {
	return b.bkt.Name()
}

type timingReadCloser struct {
	io.ReadCloser

	ok       bool
	start    time.Time
	op       string
	duration prometheus.ObserverVec
	failed   *prometheus.CounterVec
}

func newTimingReadCloser(rc io.ReadCloser, op string, dur prometheus.ObserverVec, failed *prometheus.CounterVec) *timingReadCloser {
	// Initialize the metrics with 0.
	dur.WithLabelValues(op)
	failed.WithLabelValues(op)
	return &timingReadCloser{
		ReadCloser: rc,
		ok:         true,
		start:      time.Now(),
		op:         op,
		duration:   dur,
		failed:     failed,
	}
}

func (rc *timingReadCloser) Close() error {
	err := rc.ReadCloser.Close()
	rc.duration.WithLabelValues(rc.op).Observe(time.Since(rc.start).Seconds())
	if rc.ok && err != nil {
		rc.failed.WithLabelValues(rc.op).Inc()
		rc.ok = false
	}
	return err
}

func (rc *timingReadCloser) Read(b []byte) (n int, err error) {
	n, err = rc.ReadCloser.Read(b)
	if rc.ok && err != nil && err != io.EOF {
		rc.failed.WithLabelValues(rc.op).Inc()
		rc.ok = false
	}
	return n, err
}
