package dynamodb

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util/backoff"
)

const (
	maxCasRetries = 10 // max retries in CAS operation
)

// Config to create a ConsulClient
type Config struct {
	Region         string        `yaml:"region"`
	TableName      string        `yaml:"table_name"`
	TTL            time.Duration `yaml:"ttl"`
	PullerSyncTime time.Duration `yaml:"puller_sync_time"`
	MaxCasRetries  int           `yaml:"max_cas_retries"`
	Timeout        time.Duration `yaml:"timeout"`
}

type Client struct {
	kv             dynamoDbClient
	codec          codec.Codec
	ddbMetrics     *dynamodbMetrics
	logger         log.Logger
	pullerSyncTime time.Duration
	backoffConfig  backoff.Config

	staleDataLock sync.RWMutex
	staleData     map[string]staleData
}

type staleData struct {
	data      codec.MultiKey
	timestamp time.Time
}

// RegisterFlags adds the flags required to config this to the given FlagSet
// If prefix is not an empty string it should end with a period.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Region, prefix+"dynamodb.region", "", "Region to access dynamodb.")
	f.StringVar(&cfg.TableName, prefix+"dynamodb.table-name", "", "Table name to use on dynamodb.")
	f.DurationVar(&cfg.TTL, prefix+"dynamodb.ttl-time", 0, "Time to expire items on dynamodb.")
	f.DurationVar(&cfg.PullerSyncTime, prefix+"dynamodb.puller-sync-time", 60*time.Second, "Time to refresh local ring with information on dynamodb.")
	f.IntVar(&cfg.MaxCasRetries, prefix+"dynamodb.max-cas-retries", maxCasRetries, "Maximum number of retries for DDB KV CAS.")
	f.DurationVar(&cfg.Timeout, prefix+"dynamodb.timeout", 2*time.Minute, "Timeout of dynamoDbClient requests. Default is 2m.")
}

func NewClient(cfg Config, cc codec.Codec, logger log.Logger, registerer prometheus.Registerer) (*Client, error) {
	dynamoDB, err := newDynamodbKV(cfg, logger)
	if err != nil {
		return nil, err
	}

	ddbMetrics := newDynamoDbMetrics(registerer)

	backoffConfig := backoff.Config{
		MinBackoff: 1 * time.Second,
		MaxBackoff: cfg.PullerSyncTime,
		MaxRetries: cfg.MaxCasRetries,
	}

	var kv dynamoDbClient
	kv = dynamodbInstrumentation{kv: dynamoDB, ddbMetrics: ddbMetrics}
	if cfg.Timeout > 0 {
		kv = newDynamodbKVWithTimeout(kv, cfg.Timeout)
	}
	c := &Client{
		kv:             kv,
		codec:          cc,
		logger:         ddbLog(logger),
		ddbMetrics:     ddbMetrics,
		pullerSyncTime: cfg.PullerSyncTime,
		staleData:      make(map[string]staleData),
		backoffConfig:  backoffConfig,
	}
	level.Info(c.logger).Log("msg", "dynamodb kv initialized")
	return c, nil
}

func (c *Client) List(ctx context.Context, key string) ([]string, error) {
	resp, _, err := c.kv.List(ctx, dynamodbKey{primaryKey: key})
	if err != nil {
		level.Warn(c.logger).Log("msg", "error List", "key", key, "err", err)
		return nil, err
	}
	return resp, err
}

func (c *Client) Get(ctx context.Context, key string) (interface{}, error) {
	resp, _, err := c.kv.Query(ctx, dynamodbKey{primaryKey: key}, false)
	if err != nil {
		level.Warn(c.logger).Log("msg", "error Get", "key", key, "err", err)
		return nil, err
	}
	res, err := c.decodeMultikey(resp)
	if err != nil {
		return nil, err
	}
	c.updateStaleData(key, res, time.Now().UTC())

	return res, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	resp, _, err := c.kv.Query(ctx, dynamodbKey{primaryKey: key}, false)
	if err != nil {
		level.Warn(c.logger).Log("msg", "error Delete", "key", key, "err", err)
		return err
	}

	for innerKey := range resp {
		err := c.kv.Delete(ctx, dynamodbKey{
			primaryKey: key,
			sortKey:    innerKey,
		})
		if err != nil {
			level.Warn(c.logger).Log("msg", "error Delete", "key", key, "innerKey", innerKey, "err", err)
			return err
		}
	}
	c.deleteStaleData(key)

	return err
}

func (c *Client) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	bo := backoff.New(ctx, c.backoffConfig)
	for bo.Ongoing() {
		c.ddbMetrics.dynamodbCasAttempts.Inc()
		resp, _, err := c.kv.Query(ctx, dynamodbKey{primaryKey: key}, false)
		if err != nil {
			level.Error(c.logger).Log("msg", "error cas query", "key", key, "err", err)
			bo.Wait()
			continue
		}

		current, err := c.decodeMultikey(resp)
		if err != nil {
			level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
			continue
		}

		out, retry, err := f(current.Clone())
		if err != nil {
			if !retry {
				return err
			}
			bo.Wait()
			continue
		}

		// Callback returning nil means it doesn't want to CAS anymore.
		if out == nil {
			return nil
		}
		// Don't even try

		r, ok := out.(codec.MultiKey)
		if !ok || r == nil {
			return fmt.Errorf("invalid type: %T, expected MultiKey", out)
		}

		toUpdate, toDelete, err := current.FindDifference(r)

		if err != nil {
			level.Error(c.logger).Log("msg", "error getting key", "key", key, "err", err)
			continue
		}

		buf, err := c.codec.EncodeMultiKey(toUpdate)
		if err != nil {
			level.Error(c.logger).Log("msg", "error serialising value", "key", key, "err", err)
			continue
		}

		putRequests := map[dynamodbKey][]byte{}
		for childKey, bytes := range buf {
			putRequests[dynamodbKey{primaryKey: key, sortKey: childKey}] = bytes
		}

		deleteRequests := make([]dynamodbKey, 0, len(toDelete))
		for _, childKey := range toDelete {
			deleteRequests = append(deleteRequests, dynamodbKey{primaryKey: key, sortKey: childKey})
		}

		if len(putRequests) > 0 || len(deleteRequests) > 0 {
			err = c.kv.Batch(ctx, putRequests, deleteRequests)
			if err != nil {
				return err
			}
			c.updateStaleData(key, r, time.Now().UTC())
			return nil
		}

		if len(putRequests) == 0 && len(deleteRequests) == 0 {
			// no change detected, retry
			level.Warn(c.logger).Log("msg", "no change detected in ring, retry CAS")
			bo.Wait()
			continue
		}

		return nil
	}
	err := fmt.Errorf("failed to CAS %s", key)
	level.Error(c.logger).Log("msg", "failed to CAS after retries", "key", key)
	return err
}

func (c *Client) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	bo := backoff.New(ctx, c.backoffConfig)

	for bo.Ongoing() {
		out, _, err := c.kv.Query(ctx, dynamodbKey{
			primaryKey: key,
		}, false)
		if err != nil {
			level.Error(c.logger).Log("msg", "error WatchKey", "key", key, "err", err)

			if bo.NumRetries() >= 10 {
				level.Error(c.logger).Log("msg", "failed to WatchKey after retries", "key", key, "err", err)
				if staleData := c.getStaleData(key); staleData != nil {
					if !f(staleData) {
						return
					}
				}
			}
			bo.Wait()
			continue
		}

		decoded, err := c.decodeMultikey(out)
		if err != nil {
			level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
			continue
		}
		c.updateStaleData(key, decoded, time.Now().UTC())

		if !f(decoded) {
			return
		}

		bo.Reset()
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.pullerSyncTime):
		}
	}
}

func (c *Client) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	bo := backoff.New(ctx, c.backoffConfig)

	for bo.Ongoing() {
		out, _, err := c.kv.Query(ctx, dynamodbKey{
			primaryKey: prefix,
		}, true)
		if err != nil {
			level.Error(c.logger).Log("msg", "WatchPrefix", "prefix", prefix, "err", err)
			bo.Wait()
			continue
		}

		for key, bytes := range out {
			decoded, err := c.codec.Decode(bytes)
			if err != nil {
				level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
				continue
			}
			if !f(key, decoded) {
				return
			}
		}

		bo.Reset()
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.pullerSyncTime):
		}
	}
}

func (c *Client) decodeMultikey(data map[string][]byte) (codec.MultiKey, error) {
	res, err := c.codec.DecodeMultiKey(data)
	if err != nil {
		return nil, err
	}
	out, ok := res.(codec.MultiKey)
	if !ok || out == nil {
		return nil, fmt.Errorf("invalid type: %T, expected MultiKey", out)
	}

	return out, nil
}

func (c *Client) LastUpdateTime(key string) time.Time {
	c.staleDataLock.RLock()
	defer c.staleDataLock.RUnlock()

	data, ok := c.staleData[key]
	if !ok {
		return time.Time{}
	}

	return data.timestamp
}

func (c *Client) updateStaleData(key string, data codec.MultiKey, timestamp time.Time) {
	c.staleDataLock.Lock()
	defer c.staleDataLock.Unlock()

	c.staleData[key] = staleData{
		data:      data,
		timestamp: timestamp,
	}
}

func (c *Client) getStaleData(key string) codec.MultiKey {
	c.staleDataLock.RLock()
	defer c.staleDataLock.RUnlock()

	data, ok := c.staleData[key]
	if !ok {
		return nil
	}

	newD := data.data.Clone().(codec.MultiKey)

	return newD
}

func (c *Client) deleteStaleData(key string) {
	c.staleDataLock.Lock()
	defer c.staleDataLock.Unlock()

	delete(c.staleData, key)
}

func ddbLog(logger log.Logger) log.Logger {
	return log.WithPrefix(logger, "class", "DynamodbKvClient")
}
