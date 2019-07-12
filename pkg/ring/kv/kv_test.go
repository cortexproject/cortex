package kv

import (
	"context"
	"io"
	"strconv"
	"testing"

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
