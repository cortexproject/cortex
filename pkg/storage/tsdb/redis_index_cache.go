package tsdb

import (
	"context"
	"encoding/base64"
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/labels"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"golang.org/x/crypto/blake2b"

	chunkCache "github.com/cortexproject/cortex/pkg/chunk/cache"
)

const (
	cacheTypePostings string = "Postings"
	cacheTypeSeries   string = "Series"
)

// impl thanos cache interface
var _ storecache.IndexCache = &RedisIndexCache{}

type RedisIndexCache struct {
	redisClient *chunkCache.RedisClient
	logger      log.Logger

	requests *prometheus.CounterVec
	hits     *prometheus.CounterVec
}

// NewRedisIndexCache creates a new RedisIndexCache
func NewRedisIndexCache(redisClient *chunkCache.RedisClient, logger log.Logger, reg prometheus.Registerer) (*RedisIndexCache, error) {
	r := &RedisIndexCache{
		redisClient: redisClient,
		logger:      logger,
	}

	r.requests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_requests_total",
		Help: "Total number of items requests to the cache.",
	}, []string{"item_type"})
	r.requests.WithLabelValues(cacheTypePostings)
	r.requests.WithLabelValues(cacheTypeSeries)

	r.hits = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_hits_total",
		Help: "Total number of items requests to the cache that were a hit.",
	}, []string{"item_type"})
	r.hits.WithLabelValues(cacheTypePostings)
	r.hits.WithLabelValues(cacheTypeSeries)

	return r, nil
}

func (r *RedisIndexCache) StorePostings(ctx context.Context, blockID ulid.ULID, l labels.Label, v []byte) {
	key := cacheKey(blockID, cacheKeyPostings(l))

	err := r.redisClient.MSet(ctx, []string{key}, [][]byte{v}, 0)
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to cache postings in redis", "err", err)
	}
}

func (r *RedisIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, lbls []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	hits = map[labels.Label][]byte{}

	keys := make([]string, 0, len(lbls))
	for _, lbl := range lbls {
		keys = append(keys, cacheKey(blockID, cacheKeyPostings(lbl)))
	}

	r.requests.WithLabelValues(cacheTypePostings).Add(float64(len(keys)))
	response, err := r.redisClient.MGet(ctx, keys)
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to fetch postings in redis", "err", err)
		return nil, lbls
	}

	for index, value := range response {
		label := lbls[index]
		if len(value) == 0 {
			misses = append(misses, label)
		} else {
			hits[label] = value
		}
	}

	r.hits.WithLabelValues(cacheTypePostings).Add(float64(len(hits)))
	return
}

func (r *RedisIndexCache) StoreSeries(ctx context.Context, blockID ulid.ULID, id uint64, v []byte) {
	key := cacheKey(blockID, cacheKeySeries(id))

	err := r.redisClient.MSet(ctx, []string{key}, [][]byte{v}, 0)
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to cache series in redis", "err", err)
	}
}

func (r *RedisIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []uint64) (hits map[uint64][]byte, misses []uint64) {
	hits = map[uint64][]byte{}

	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, cacheKey(blockID, cacheKeySeries(id)))
	}

	r.requests.WithLabelValues(cacheTypeSeries).Add(float64(len(keys)))

	response, err := r.redisClient.MGet(ctx, keys)
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to fetch postings in redis", "err", err)
		return nil, ids
	}

	for index, value := range response {
		id := ids[index]
		if len(value) == 0 {
			misses = append(misses, id)
		} else {
			hits[id] = value
		}
	}

	r.hits.WithLabelValues(cacheTypeSeries).Add(float64(len(hits)))
	return
}

type cacheKeyPostings labels.Label
type cacheKeySeries uint64

func cacheKey(block ulid.ULID, key interface{}) string {
	switch k := key.(type) {
	case cacheKeyPostings:
		lbl := k
		lblHash := blake2b.Sum256([]byte(lbl.Name + ":" + lbl.Value))
		return "P:" + block.String() + ":" + base64.RawURLEncoding.EncodeToString(lblHash[0:])
	case cacheKeySeries:
		return "S:" + block.String() + ":" + strconv.FormatUint(uint64(k), 10)
	default:
		return ""
	}
}
