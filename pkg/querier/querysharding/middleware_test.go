package querysharding

import (
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"time"
)

func defaultReq() *queryrange.Request {
	return &queryrange.Request{
		Path:    "/query_range",
		Start:   10,
		End:     20,
		Step:    5,
		Timeout: time.Minute,
		Query:   `__embedded_query__{__cortex_query__="687474705f72657175657374735f746f74616c7b636c75737465723d2270726f64227d"}`,
	}
}
