package storegateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Part struct {
	Start uint64
	End   uint64

	ElemRng [2]int
}

type Partitioner interface {
	// Partition partitions length entries into n <= length ranges that cover all
	// input ranges
	// It supports overlapping ranges.
	// NOTE: It expects range to be sorted by start time.
	Partition(length int, rng func(int) (uint64, uint64)) []Part
}

type gapBasedPartitioner struct {
	maxGapSize uint64

	// Metrics.
	requestedBytes  prometheus.Counter
	requestedRanges prometheus.Counter
	expandedBytes   prometheus.Counter
	expandedRanges  prometheus.Counter
}

func newGapBasedPartitioner(maxGapBytes uint64, reg prometheus.Registerer) Partitioner {
	return &gapBasedPartitioner{
		maxGapSize: maxGapBytes,
		requestedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_requested_bytes_total",
			Help: "Total size of byte ranges required to fetch from the storage before they are passed to the partitioner.",
		}),
		expandedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_expanded_bytes_total",
			Help: "Total size of byte ranges returned by the partitioner after they've been combined together to reduce the number of bucket API calls.",
		}),
		requestedRanges: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_requested_ranges_total",
			Help: "Total number of byte ranges required to fetch from the storage before they are passed to the partitioner.",
		}),
		expandedRanges: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_expanded_ranges_total",
			Help: "Total number of byte ranges returned by the partitioner after they've been combined together to reduce the number of bucket API calls.",
		}),
	}
}

// Partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
// It is used to combine multiple small ranges from object storage into bigger, more efficient/cheaper ones.
func (g gapBasedPartitioner) partitionInternal(length int, rng func(int) (uint64, uint64)) (parts []Part) {
	j := 0
	k := 0
	for k < length {
		j = k
		k++

		p := Part{}
		p.Start, p.End = rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if p.End+g.maxGapSize < s {
				break
			}

			if p.End <= e {
				p.End = e
			}
		}
		p.ElemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return parts
}

func (p *gapBasedPartitioner) Partition(length int, rng func(int) (uint64, uint64)) []Part {
	// Calculate the size of requested ranges.
	requestedBytes := uint64(0)
	for i := 0; i < length; i++ {
		start, end := rng(i)
		requestedBytes += end - start
	}

	// Run the upstream partitioner to compute the actual ranges that will be fetched.
	parts := p.partitionInternal(length, rng)

	// Calculate the size of ranges that will be fetched.
	expandedBytes := uint64(0)
	for _, p := range parts {
		expandedBytes += p.End - p.Start
	}

	p.requestedBytes.Add(float64(requestedBytes))
	p.expandedBytes.Add(float64(expandedBytes))
	p.requestedRanges.Add(float64(length))
	p.expandedRanges.Add(float64(len(parts)))

	return parts
}
