package tripperware

import (
	"time"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

// Limits allows us to specify per-tenant runtime limits on the behavior of
// the query handling code.
type Limits interface {
	// MaxQueryLookback returns the max lookback period of queries.
	MaxQueryLookback(userID string) time.Duration

	// MaxQueryLength returns the limit of the length (in time) of a query.
	MaxQueryLength(string) time.Duration

	// MaxQueryParallelism returns the limit to the number of split queries the
	// frontend will process in parallel.
	MaxQueryParallelism(string) int

	// MaxQueryResponseSize returns the max total response size of a query in bytes.
	MaxQueryResponseSize(string) int64

	// MaxCacheFreshness returns the period after which results are cacheable,
	// to prevent caching of very recent results.
	MaxCacheFreshness(string) time.Duration

	// QueryVerticalShardSize returns the maximum number of queriers that can handle requests for this user.
	QueryVerticalShardSize(userID string) int

	// QueryPriority returns the query priority config for the tenant, including different priorities and their attributes.
	QueryPriority(userID string) validation.QueryPriority

	QueryRejection(userID string) validation.QueryRejection
}
