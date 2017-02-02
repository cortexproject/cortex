package util

import (
	"hash/fnv"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
)

// HashBucketHistogramOpts are the options for making a HashBucketHistogram
type HashBucketHistogramOpts struct {
	prometheus.HistogramOpts
	HashBuckets int
}

// HashBucketHistogram is used to track a histogram of per-bucket rates.
//
// For instance, I want to know that 50% of rows are getting X QPS or lower
// and 99% are getting Y QPS of lower.  At first glance, this would involve
// tracking write rate per row, and periodically sticking those numbers in
// a histogram.  To make this fit in memory: instead of per-row, we keep
// N buckets of counters and hash the key to a bucket.  Then every scrape
// we update a histogram with the bucket values (and zero the buckets).
type HashBucketHistogram interface {
	prometheus.Metric
	prometheus.Collector

	Observe(string, uint32)
}

type hashBucketHistogram struct {
	prometheus.Histogram
	buckets []uint32
}

// NewHashBucketHistogram makes a new HashBucketHistogram
func NewHashBucketHistogram(opts HashBucketHistogramOpts) HashBucketHistogram {
	return &hashBucketHistogram{
		Histogram: prometheus.NewHistogram(opts.HistogramOpts),
		buckets:   make([]uint32, opts.HashBuckets, opts.HashBuckets),
	}
}

// Collect implements prometheus.Metric
func (h *hashBucketHistogram) Collect(c chan<- prometheus.Metric) {
	for i := range h.buckets {
		h.Histogram.Observe(float64(atomic.SwapUint32(&h.buckets[i], 0)))
	}
	h.Histogram.Collect(c)
}

// Observe implements HashBucketHistogram
func (h *hashBucketHistogram) Observe(key string, value uint32) {
	hash := fnv.New32()
	hash.Write(bytesView(key))
	i := hash.Sum32() % uint32(len(h.buckets))
	atomic.AddUint32(&h.buckets[i], value)
}

func bytesView(v string) []byte {
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&v))
	bytesHeader := reflect.SliceHeader{
		Data: strHeader.Data,
		Len:  strHeader.Len,
		Cap:  strHeader.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bytesHeader))
}
