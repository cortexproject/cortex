package rueidis

import (
	"context"
	"sync"
	"time"

	"github.com/rueian/rueidis/internal/cmds"
	"github.com/rueian/rueidis/internal/util"
)

// MGetCache is a helper that consults the client-side caches with multiple keys by grouping keys within same slot into MGETs
func MGetCache(client Client, ctx context.Context, ttl time.Duration, keys []string) (ret map[string]RedisMessage, err error) {
	if len(keys) == 0 {
		return make(map[string]RedisMessage), nil
	}
	ret = make(map[string]RedisMessage, len(keys))
	if cc, ok := client.(*clusterClient); ok {
		return clusterMGetCache(cc, ctx, ttl, cmds.MGets(keys), keys)
	}
	return clientMGetCache(client, ctx, ttl, client.B().Mget().Key(keys...).Cache(), keys)
}

// JsonMGetCache is a helper that consults the client-side caches with multiple keys by grouping keys within same slot into JSON.MGETs
func JsonMGetCache(client Client, ctx context.Context, ttl time.Duration, keys []string, path string) (ret map[string]RedisMessage, err error) {
	if len(keys) == 0 {
		return make(map[string]RedisMessage), nil
	}
	if cc, ok := client.(*clusterClient); ok {
		return clusterMGetCache(cc, ctx, ttl, cmds.JsonMGets(keys, path), keys)
	}
	return clientMGetCache(client, ctx, ttl, client.B().JsonMget().Key(keys...).Path(path).Cache(), keys)
}

func clientMGetCache(client Client, ctx context.Context, ttl time.Duration, cmd cmds.Cacheable, keys []string) (ret map[string]RedisMessage, err error) {
	arr, err := client.DoCache(ctx, cmd, ttl).ToArray()
	if err != nil {
		return nil, err
	}
	ret = make(map[string]RedisMessage, len(keys))
	for i, resp := range arr {
		ret[keys[i]] = resp
	}
	return ret, nil
}

func clusterMGetCache(cc *clusterClient, ctx context.Context, ttl time.Duration, mgets map[uint16]cmds.Completed, keys []string) (ret map[string]RedisMessage, err error) {
	var mu sync.Mutex
	ret = make(map[string]RedisMessage, len(keys))
	util.ParallelVals(mgets, func(cmd cmds.Completed) {
		c := cmds.Cacheable(cmd)
		arr, err2 := cc.doCache(ctx, c, ttl).ToArray()
		mu.Lock()
		if err2 != nil {
			err = err2
		} else {
			for i, resp := range arr {
				ret[c.MGetCacheKey(i)] = resp
			}
		}
		mu.Unlock()
	})
	if err != nil {
		return nil, err
	}
	for _, mget := range mgets { // not recycle cmds if error, since cmds may be used later in pipe. consider recycle them by pipe
		cmds.Put(mget.CommandSlice())
	}
	return ret, nil
}
