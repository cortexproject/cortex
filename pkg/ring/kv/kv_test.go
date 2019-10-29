package kv

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ring/kv/etcd"
)

func withFixtures(t *testing.T, f func(*testing.T, Client)) {
	for _, fixture := range []struct {
		name    string
		factory func() (Client, io.Closer, error)
	}{
		{"consul", func() (Client, io.Closer, error) {
			return consul.NewInMemoryClient(codec.String{}), etcd.NopCloser, nil
		}},
		{"etcd", func() (Client, io.Closer, error) {
			return etcd.Mock(codec.String{})
		}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			client, closer, err := fixture.factory()
			require.NoError(t, err)
			defer closer.Close()
			f(t, client)
		})
	}
}

var (
	ctx = context.Background()
	key = "/key"
)

func TestCAS(t *testing.T) {
	withFixtures(t, func(t *testing.T, client Client) {
		// Blindly set key to "0".
		err := client.CAS(ctx, key, func(in interface{}) (interface{}, bool, error) {
			return "0", true, nil
		})
		require.NoError(t, err)

		// Swap key to i+1 iff its i.
		for i := 0; i < 10; i++ {
			err = client.CAS(ctx, key, func(in interface{}) (interface{}, bool, error) {
				require.EqualValues(t, strconv.Itoa(i), in)
				return strconv.Itoa(i + 1), true, nil
			})
			require.NoError(t, err)
		}

		// Make sure the CASes left the right value - "10".
		value, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.EqualValues(t, "10", value)
	})
}

// TestNilCAS ensures we can return nil from the CAS callback when we don't
// want to modify the value.
func TestNilCAS(t *testing.T) {
	withFixtures(t, func(t *testing.T, client Client) {
		// Blindly set key to "0".
		err := client.CAS(ctx, key, func(in interface{}) (interface{}, bool, error) {
			return "0", true, nil
		})
		require.NoError(t, err)

		// Ensure key is "0" and don't set it.
		err = client.CAS(ctx, key, func(in interface{}) (interface{}, bool, error) {
			require.EqualValues(t, "0", in)
			return nil, false, nil
		})
		require.NoError(t, err)

		// Make sure value is still 0.
		value, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.EqualValues(t, "0", value)
	})
}

func TestWatchKey(t *testing.T) {
	const key = "test"
	const max = 100
	const sleep = 10 * time.Millisecond

	withFixtures(t, func(t *testing.T, client Client) {
		ch := make(chan struct{})
		go func() {
			defer close(ch)
			for i := 0; i < max; i++ {
				// Start with sleeping, so that watching client see empty KV store at the beginning.
				time.Sleep(sleep)

				err := client.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
					return fmt.Sprintf("%d", i), true, nil
				})
				require.NoError(t, err)
			}
		}()

		observedValues := map[string]time.Time{}
		ctx, cancel := context.WithTimeout(context.Background(), 1.5*max*sleep)
		defer cancel()

		client.WatchKey(ctx, key, func(i interface{}) bool {
			observedValues[i.(string)] = time.Now()
			return true
		})

		// wait until updater finishes (should be done by now)
		<-ch

		// We should have seen some values, observed at increasing times
		count := 0
		lastVal := ""
		lastTime := time.Time{}
		for i := 0; i < max; i++ {
			val := fmt.Sprintf("%d", i)
			ot, ok := observedValues[val]
			if ok {
				count++
				if lastTime.After(ot) {
					t.Errorf("value %s observed at %s, before previous value %s observed at %s", val, ot, lastVal, lastTime)
				}
			}
		}

		const expectedFactor = 0.9
		if count < expectedFactor*max {
			t.Errorf("expected at least %.0f%% observed values, got %.0f%% (observed count: %d)", 100*expectedFactor, 100*float64(count)/max, count)
		}
	})
}

func TestWatchPrefix(t *testing.T) {
	withFixtures(t, func(t *testing.T, client Client) {
		const prefix = "test/"
		const prefix2 = "ignore/"

		const max = 100
		const sleep = time.Millisecond * 10

		gen := func(p string, ch chan struct{}) {
			defer close(ch)
			for i := 0; i < max; i++ {
				// Start with sleeping, so that watching client see empty KV store at the beginning.
				time.Sleep(sleep)

				key := fmt.Sprintf("%s%d", p, i)
				err := client.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
					return key, true, nil
				})
				require.NoError(t, err)
			}
		}

		ch1 := make(chan struct{})
		ch2 := make(chan struct{})
		go gen(prefix, ch1)
		go gen(prefix2, ch2) // we don't want to see these keys reported

		observedKeys := map[string]int{}
		ctx, cfn := context.WithTimeout(context.Background(), 1.5*max*sleep)
		defer cfn()

		client.WatchPrefix(ctx, prefix, func(key string, val interface{}) bool {
			observedKeys[key] = observedKeys[key] + 1
			return true
		})

		// wait until updaters finish (should be done by now)
		<-ch1
		<-ch2

		// verify that each key was reported once, and keys outside prefix were not reported
		for i := 0; i < max; i++ {
			key := fmt.Sprintf("%s%d", prefix, i)

			if observedKeys[key] != 1 {
				t.Errorf("key %s has incorrect value %d", key, observedKeys[key])
			}
			delete(observedKeys, key)
		}

		if len(observedKeys) > 0 {
			t.Errorf("unexpected keys reported: %v", observedKeys)
		}
	})
}
