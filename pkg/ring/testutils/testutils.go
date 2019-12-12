package testutils

import (
	"context"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

// NumTokens determines the number of tokens owned by the specified
// address
func NumTokens(c kv.Client, name, ringKey string) int {
	ringDesc, err := c.Get(context.Background(), ringKey)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error reading consul", "err", err)
		return 0
	}
	count := 0
	rd := ringDesc.(*ring.Desc)
	for _, token := range rd.Tokens {
		if token.Ingester == name {
			count++
		}
	}
	return count + len(rd.Ingesters[name].Tokens)
}
