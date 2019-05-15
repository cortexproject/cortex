package testutils

import (
	"context"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kvstore"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

// NumTokens determines the number of tokens owned by the specified
// address
func NumTokens(c kvstore.KVClient, name string) int {
	ringDesc, err := c.Get(context.Background(), ring.ConsulKey)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error reading consul", "err", err)
		return 0
	}
	count := 0
	for _, token := range ringDesc.(*ring.Desc).Tokens {
		if token.Ingester == name {
			count++
		}
	}
	return count
}
