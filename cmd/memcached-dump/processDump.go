package main

import (
	"fmt"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
)

func processDump(req *queryrange.CachedResponse, b []byte) {
	fmt.Println(string(b))
}
