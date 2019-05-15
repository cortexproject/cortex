package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/ring/kvstore"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/etcd-io/etcd/clientv3"
	"github.com/go-kit/kit/log/level"
)

// Config for a new etcd.Client.
type Config struct {
	Endpoints   []string
	DialTimeout time.Duration
	Retries     int
}

// New makes a new Client.
func New(cfg Config, codec kvstore.Codec) (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:   cfg,
		codec: codec,
		cli:   cli,
	}, nil
}

// Client implements ring.KVClient for etcd.
type Client struct {
	cfg   Config
	codec kvstore.Codec
	cli   *clientv3.Client
}

// CAS implements ring.KVClient.
func (c *Client) CAS(ctx context.Context, key string, f kvstore.CASCallback) error {
	var revision int64

	for i := 0; i < c.cfg.Retries; i++ {
		resp, err := c.cli.Get(ctx, key)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error getting key", "key", key, "err", err)
			continue
		}

		var intermediate interface{}
		if len(resp.Kvs) > 0 {
			intermediate, err = c.codec.Decode(resp.Kvs[0].Value)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error decoding key", "key", key, "err", err)
				continue
			}
			revision = resp.Kvs[0].ModRevision
		}

		var retry bool
		intermediate, retry, err = f(intermediate)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error CASing", "key", key, "err", err)
			if !retry {
				return err
			}
			continue
		}

		if intermediate == nil {
			panic("Callback must instantiate value!")
		}

		buf, err := c.codec.Encode(intermediate)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error serialising value", "key", key, "err", err)
			continue
		}

		_, err = c.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(key), "=", revision)).
			Then(clientv3.OpPut(key, string(buf))).
			Commit()

		if err != nil {
			level.Error(util.Logger).Log("msg", "error CASing", "key", key, "err", err)
			continue
		}

		return nil
	}
	return fmt.Errorf("failed to CAS %s", key)
}

// WatchKey implements ring.KVClient.
func (c *Client) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	backoff := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 1 * time.Minute,
	})
	for backoff.Ongoing() {
		watchChan := c.cli.Watch(ctx, key)
		for {
			resp, ok := <-watchChan
			if !ok {
				break
			}
			backoff.Reset()

			for _, event := range resp.Events {
				out, err := c.codec.Decode(event.Kv.Value)
				if err != nil {
					level.Error(util.Logger).Log("msg", "error decoding key", "key", key, "err", err)
					continue
				}

				if !f(out) {
					return
				}
			}
		}
	}
}

// Get implements ring.KVClient.
func (c *Client) Get(ctx context.Context, key string) (interface{}, error) {
	resp, err := c.cli.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) != 0 {
		return nil, fmt.Errorf("got %d kvs, expected 1", len(resp.Kvs))
	}
	return c.codec.Decode(resp.Kvs[0].Value)
}

// PutBytes implements ring.KVClient.
func (c *Client) PutBytes(ctx context.Context, key string, buf []byte) error {
	_, err := c.cli.Put(ctx, key, string(buf))
	return err
}
