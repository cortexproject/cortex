package consul

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/kit/log/level"
	consul "github.com/hashicorp/consul/api"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util"
)

type stringCodec struct{}

func (c stringCodec) Encode(d interface{}) ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("nil")
	}
	s, ok := d.(string)
	if !ok {
		return nil, fmt.Errorf("not string: %T", d)
	}

	return []byte(s), nil
}

func (c stringCodec) Decode(d []byte) (interface{}, error) {
	return string(d), nil
}

var _ codec.Codec = &stringCodec{}

func writeValuesToKV(client *Client, key string, start, end int, sleep time.Duration) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for i := start; i <= end; i++ {
			_, _ = client.Put(&consul.KVPair{Key: key, Value: []byte(fmt.Sprintf("%d", i))}, nil)
			time.Sleep(sleep)
		}
	}()
	return ch
}

func TestWatchKey(t *testing.T) {
	c := NewInMemoryClientWithConfig(&stringCodec{}, Config{
		WatchKeyRate:  5.0,
		WatchKeyBurst: 1,
	})

	const key = "test"
	const max = 100

	// Make sure to start with non-empty value, otherwise WatchKey will bail
	_, _ = c.Put(&consul.KVPair{Key: key, Value: []byte("start")}, nil)

	ch := writeValuesToKV(c, key, 0, max, 10*time.Millisecond)

	observed := observeValueForSomeTime(c, key, 1200*time.Millisecond) // little over 1 second

	// wait until updater finishes
	<-ch

	if testing.Verbose() {
		t.Log(observed)
	}
	// Let's see how many updates we have observed. Given the rate limit and our observing time, it should be 6
	// We should also have seen one of the later values, as we're observing for longer than a second, so rate limit should allow
	// us to see it.
	if len(observed) < 5 || len(observed) > 10 {
		t.Error("Expected ~6 observed values, got", observed)
	}
	last := observed[len(observed)-1]
	n, _ := strconv.Atoi(last)
	if n < max/2 {
		t.Error("Expected to see high last observed value, got", observed)
	}
}

func TestWatchKeyNoRateLimit(t *testing.T) {
	c := NewInMemoryClientWithConfig(&stringCodec{}, Config{
		WatchKeyRate: 0,
	})

	const key = "test"
	const max = 100

	// Make sure to start with non-empty value, otherwise WatchKey will bail
	_, _ = c.Put(&consul.KVPair{Key: key, Value: []byte("start")}, nil)

	ch := writeValuesToKV(c, key, 0, max, time.Millisecond)
	observed := observeValueForSomeTime(c, key, 500*time.Millisecond)

	// wait until updater finishes
	<-ch

	// With no limit, we should see most written values (we can lose some values if watching
	// code is busy while multiple new values are written)
	if len(observed) < 3*max/4 {
		t.Error("Expected at least 3/4 of all values, got", observed)
	}
}

func TestReset(t *testing.T) {
	c := NewInMemoryClientWithConfig(&stringCodec{}, Config{
		WatchKeyRate:  1,
		WatchKeyBurst: 5,
	})

	const key = "test"
	const max = 5

	// Make sure to start with non-empty value, otherwise WatchKey will bail
	_, _ = c.Put(&consul.KVPair{Key: key, Value: []byte("start")}, nil)

	ch := make(chan error)
	go func() {
		defer close(ch)
		for i := 0; i <= max; i++ {
			_, _ = c.Put(&consul.KVPair{Key: key, Value: []byte(fmt.Sprintf("%d", i))}, nil)
			if i == 1 {
				c.kv.(*mockKV).ResetIndex()
			}
			if i == 2 {
				c.kv.(*mockKV).ResetIndexForKey(key)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	observed := observeValueForSomeTime(c, key, 1200*time.Millisecond) // little over 1 second

	// wait until updater finishes
	<-ch

	// Let's see how many updates we have observed. Given the rate limit and our observing time, it should be 6
	if testing.Verbose() {
		t.Log(observed)
	}
	if len(observed) < 5 {
		t.Error("Expected at least 5 observed values, got", observed)
	} else if observed[len(observed)-1] != fmt.Sprintf("%d", max) {
		t.Error("Expected to see last written value, got", observed)
	}
}

func observeValueForSomeTime(client *Client, key string, timeout time.Duration) []string {
	observed := []string(nil)
	ctx, cancel := context.WithTimeout(context.Background(), timeout) // little over 1 second
	defer cancel()
	client.WatchKey(ctx, key, func(i interface{}) bool {
		s, ok := i.(string)
		if !ok {
			return false
		}
		level.Debug(util.Logger).Log("msg", "observed value", "val", s, "time", time.Now())
		observed = append(observed, s)
		return true
	})
	return observed
}
