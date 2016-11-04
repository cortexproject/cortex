// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ring

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	consul "github.com/hashicorp/consul/api"
	"github.com/prometheus/common/log"
)

const (
	longPollDuration = 10 * time.Second
)

// ConsulClient is a high-level client for Consul, that exposes operations
// such as CAS and Watch which take callbacks.  It also deals with serialisation
// by having an instance factory passed in to methods and deserialising into that.
type ConsulClient interface {
	CAS(key string, factory InstanceFactory, f CASCallback) error
	WatchKey(key string, factory InstanceFactory, done <-chan struct{}, f func(interface{}) bool)
	// XXX: The following three are unused in the code-base. Can we delete them?
	Get(key string, factory InstanceFactory) error
	WatchPrefix(path string, factory InstanceFactory, done <-chan struct{}, f func(string, interface{}) bool)
	PutBytes(key string, buf []byte) error
}

// CASCallback is the type of the callback to CAS.  If err is nil, out must be non-nil.
type CASCallback func(in interface{}) (out interface{}, retry bool, err error)

// InstanceFactory type creates empty instances for use when deserialising
type InstanceFactory func() interface{}

type kv interface {
	CAS(p *consul.KVPair, q *consul.WriteOptions) (bool, *consul.WriteMeta, error)
	Get(key string, q *consul.QueryOptions) (*consul.KVPair, *consul.QueryMeta, error)
	List(path string, q *consul.QueryOptions) (consul.KVPairs, *consul.QueryMeta, error)
	Put(p *consul.KVPair, q *consul.WriteOptions) (*consul.WriteMeta, error)
}

type consulClient struct {
	kv
}

// NewConsulClient returns a new ConsulClient.
func NewConsulClient(addr string) (ConsulClient, error) {
	client, err := consul.NewClient(&consul.Config{
		Address: addr,
		Scheme:  "http",
	})
	if err != nil {
		return nil, err
	}
	return &consulClient{client.KV()}, nil
}

var (
	queryOptions = &consul.QueryOptions{
		RequireConsistent: true,
	}
	writeOptions = &consul.WriteOptions{}

	// ErrNotFound is returned by ConsulClient.Get.
	ErrNotFound = fmt.Errorf("Not found")
)

// Get and deserialise a JSON value from Consul.
func (c *consulClient) Get(key string, factory InstanceFactory) error {
	kvp, _, err := c.kv.Get(key, queryOptions)
	if err != nil {
		return err
	}
	if kvp == nil {
		return ErrNotFound
	}
	out := factory()
	if err := json.NewDecoder(bytes.NewReader(kvp.Value)).Decode(out); err != nil {
		return err
	}
	return nil
}

// CAS atomically modifies a value in a callback.
// If value doesn't exist you'll get nil as an argument to your callback.
func (c *consulClient) CAS(key string, factory InstanceFactory, f CASCallback) error {
	var (
		index   = uint64(0)
		retries = 10
		retry   = true
	)
	for i := 0; i < retries; i++ {
		kvp, _, err := c.kv.Get(key, queryOptions)
		if err != nil {
			log.Errorf("Error getting %s: %v", key, err)
			continue
		}
		var intermediate interface{}
		if kvp != nil {
			out := factory()
			if err := json.NewDecoder(bytes.NewReader(kvp.Value)).Decode(out); err != nil {
				log.Errorf("Error deserialising %s: %v", key, err)
				continue
			}
			// If key doesn't exist, index will be 0.
			index = kvp.ModifyIndex
			intermediate = out
		}

		intermediate, retry, err = f(intermediate)
		if err != nil {
			log.Errorf("Error CASing %s: %v", key, err)
			if !retry {
				return err
			}
			continue
		}

		if intermediate == nil {
			panic("Callback must instantiate value!")
		}

		value := bytes.Buffer{}
		if err := json.NewEncoder(&value).Encode(intermediate); err != nil {
			log.Errorf("Error serialising value for %s: %v", key, err)
			continue
		}
		ok, _, err := c.kv.CAS(&consul.KVPair{
			Key:         key,
			Value:       value.Bytes(),
			ModifyIndex: index,
		}, writeOptions)
		if err != nil {
			log.Errorf("Error CASing %s: %v", key, err)
			continue
		}
		if !ok {
			log.Errorf("Error CASing %s, trying again %d", key, index)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to CAS %s", key)
}

const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 1 * time.Minute
)

type backoff struct {
	done    <-chan struct{}
	backoff time.Duration
}

func newBackoff(done <-chan struct{}) *backoff {
	return &backoff{
		done:    done,
		backoff: initialBackoff,
	}
}

func (b *backoff) reset() {
	b.backoff = initialBackoff
}

func (b *backoff) wait() {
	select {
	case <-b.done:
	case <-time.After(b.backoff):
		b.backoff = b.backoff * 2
		if b.backoff > maxBackoff {
			b.backoff = maxBackoff
		}
	}
}

func isClosed(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// WatchPrefix will watch a given prefix in consul for changes. When a value
// under said prefix changes, the f callback is called with the deserialised
// value. To construct the deserialised value, a factory function should be
// supplied which generates an empty struct for WatchPrefix to deserialise
// into. Values in Consul are assumed to be JSON. This function blocks until
// the done channel is closed.
func (c *consulClient) WatchPrefix(prefix string, factory InstanceFactory, done <-chan struct{}, f func(string, interface{}) bool) {
	var (
		backoff = newBackoff(done)
		index   = uint64(0)
	)
	for {
		if isClosed(done) {
			return
		}
		kvps, meta, err := c.kv.List(prefix, &consul.QueryOptions{
			RequireConsistent: true,
			WaitIndex:         index,
			WaitTime:          longPollDuration,
		})
		if err != nil {
			log.Errorf("Error getting path %s: %v", prefix, err)
			backoff.wait()
			continue
		}
		backoff.reset()

		// Skip if the index is the same as last time, because the key value is
		// guaranteed to be the same as last time
		if index == meta.LastIndex {
			continue
		}
		index = meta.LastIndex

		for _, kvp := range kvps {
			out := factory()
			if err := json.NewDecoder(bytes.NewReader(kvp.Value)).Decode(out); err != nil {
				log.Errorf("Error deserialising %s: %v", kvp.Key, err)
				continue
			}
			if !f(kvp.Key, out) {
				return
			}
		}
	}
}

// WatchKey will watch a given key in consul for changes. When the value
// under said key changes, the f callback is called with the deserialised
// value. To construct the deserialised value, a factory function should be
// supplied which generates an empty struct for WatchKey to deserialise
// into. Values in Consul are assumed to be JSON. This function blocks until
// the done channel is closed.
func (c *consulClient) WatchKey(key string, factory InstanceFactory, done <-chan struct{}, f func(interface{}) bool) {
	var (
		backoff = newBackoff(done)
		index   = uint64(0)
	)
	for {
		if isClosed(done) {
			return
		}
		kvp, meta, err := c.kv.Get(key, &consul.QueryOptions{
			RequireConsistent: true,
			WaitIndex:         index,
			WaitTime:          longPollDuration,
		})
		if err != nil {
			log.Errorf("Error getting path %s: %v", key, err)
			backoff.wait()
			continue
		}
		backoff.reset()

		// Skip if the index is the same as last time, because the key value is
		// guaranteed to be the same as last time
		if index == meta.LastIndex {
			continue
		}
		index = meta.LastIndex

		var out interface{}
		if kvp != nil {
			out = factory()
			if err := json.NewDecoder(bytes.NewReader(kvp.Value)).Decode(out); err != nil {
				log.Errorf("Error deserialising %s: %v", kvp.Key, err)
				continue
			}
		}
		if !f(out) {
			return
		}
	}
}

func (c *consulClient) PutBytes(key string, buf []byte) error {
	_, err := c.kv.Put(&consul.KVPair{
		Key:   key,
		Value: buf,
	}, &consul.WriteOptions{})
	return err
}

type prefixedConsulClient struct {
	prefix string
	consul ConsulClient
}

// PrefixClient takes a ConsulClient and forces a prefix on all its operations.
func PrefixClient(client ConsulClient, prefix string) ConsulClient {
	return &prefixedConsulClient{prefix, client}
}

// Get and deserialise a JSON value from Consul.
func (c *prefixedConsulClient) Get(key string, factory InstanceFactory) error {
	return c.consul.Get(c.prefix+key, factory)
}

// CAS atomically modifies a value in a callback. If the value doesn't exist,
// you'll get 'nil' as an argument to your callback.
func (c *prefixedConsulClient) CAS(key string, factory InstanceFactory, f CASCallback) error {
	return c.consul.CAS(c.prefix+key, factory, f)
}

// WatchPrefix watches a prefix. This is in addition to the prefix we already have.
func (c *prefixedConsulClient) WatchPrefix(path string, factory InstanceFactory, done <-chan struct{}, f func(string, interface{}) bool) {
	c.consul.WatchPrefix(c.prefix+path, factory, done, f)
}

// WatchKey watches a key.
func (c *prefixedConsulClient) WatchKey(key string, factory InstanceFactory, done <-chan struct{}, f func(interface{}) bool) {
	c.consul.WatchKey(c.prefix+key, factory, done, f)
}

// PutBytes writes bytes to Consul.
func (c *prefixedConsulClient) PutBytes(key string, buf []byte) error {
	return c.consul.PutBytes(c.prefix+key, buf)
}
