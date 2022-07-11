//go:build requires_docker
// +build requires_docker

package integration

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/ring/kv/etcd"
)

func TestKVList(t *testing.T) {
	testKVs(t, func(t *testing.T, client kv.Client, reg *prometheus.Registry) {
		// Create keys to list back
		keysToCreate := []string{"key-a", "key-b", "key-c"}
		for _, key := range keysToCreate {
			err := client.CAS(context.Background(), key, func(in interface{}) (out interface{}, retry bool, err error) {
				return key, false, nil
			})
			require.NoError(t, err, "could not create key")
		}

		// Get list of keys and sort them
		keys, err := client.List(context.Background(), "")
		require.NoError(t, err, "could not list keys")
		sort.Strings(keys)
		require.Equal(t, keysToCreate, keys, "returned key paths did not match created paths")

		verifyClientMetricsHistogram(t, reg, "cortex_kv_request_duration_seconds", map[string]uint64{
			"List": 1,
			"CAS":  3,
		})
	})
}

func TestKVDelete(t *testing.T) {
	testKVs(t, func(t *testing.T, client kv.Client, reg *prometheus.Registry) {
		// Create a key
		err := client.CAS(context.Background(), "key-to-delete", func(in interface{}) (out interface{}, retry bool, err error) {
			return "key-to-delete", false, nil
		})
		require.NoError(t, err, "object could not be created")

		// Now delete it
		err = client.Delete(context.Background(), "key-to-delete")
		require.NoError(t, err)

		// Get it back
		v, err := client.Get(context.Background(), "key-to-delete")
		require.NoError(t, err, "unexpected error")
		require.Nil(t, v, "object was not deleted")

		verifyClientMetricsHistogram(t, reg, "cortex_kv_request_duration_seconds", map[string]uint64{
			"Delete": 1,
			"CAS":    1,
			"GET":    1,
		})
	})
}

func TestKVWatchAndDelete(t *testing.T) {
	testKVs(t, func(t *testing.T, client kv.Client, reg *prometheus.Registry) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := client.CAS(context.Background(), "key-before-watch", func(in interface{}) (out interface{}, retry bool, err error) {
			return "value-before-watch", false, nil
		})
		require.NoError(t, err)

		w := &watcher{}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.watch(ctx, client)
		}()

		err = client.CAS(context.Background(), "key-to-delete", func(in interface{}) (out interface{}, retry bool, err error) {
			return "value-to-delete", false, nil
		})
		require.NoError(t, err, "object could not be created")

		// Give watcher time to receive notification.
		time.Sleep(500 * time.Millisecond)

		// Now delete it
		err = client.Delete(context.Background(), "key-to-delete")
		require.NoError(t, err)

		// Give watcher time to receive notification for delete, if any.
		time.Sleep(500 * time.Millisecond)

		// Stop the watcher
		cancel()
		wg.Wait()

		// Consul reports:
		// map[key-before-watch:[value-before-watch] key-to-delete:[value-to-delete]]
		//
		// Etcd reports (before changing etcd client to ignore deletes):
		// map[key-to-delete:[value-to-delete <nil>]]
		t.Log(w.values)
	})
}

func setupEtcd(t *testing.T, scenario *e2e.Scenario, reg prometheus.Registerer, logger log.Logger) kv.Client {
	t.Helper()

	etcdSvc := e2edb.NewETCD()
	require.NoError(t, scenario.StartAndWaitReady(etcdSvc))

	etcdKv, err := kv.NewClient(kv.Config{
		Store:  "etcd",
		Prefix: "keys/",
		StoreConfig: kv.StoreConfig{
			Etcd: etcd.Config{
				Endpoints:   []string{etcdSvc.HTTPEndpoint()},
				DialTimeout: time.Minute,
				MaxRetries:  5,
			},
		},
	}, stringCodec{}, reg, logger)
	require.NoError(t, err)

	return etcdKv
}

func setupConsul(t *testing.T, scenario *e2e.Scenario, reg prometheus.Registerer, logger log.Logger) kv.Client {
	t.Helper()

	consulSvc := e2edb.NewConsul()
	require.NoError(t, scenario.StartAndWaitReady(consulSvc))

	consulKv, err := kv.NewClient(kv.Config{
		Store:  "consul",
		Prefix: "keys/",
		StoreConfig: kv.StoreConfig{
			Consul: consul.Config{
				Host:              consulSvc.HTTPEndpoint(),
				HTTPClientTimeout: time.Minute,
				WatchKeyBurstSize: 5,
				WatchKeyRateLimit: 1,
			},
		},
	}, stringCodec{}, reg, logger)
	require.NoError(t, err)

	return consulKv
}

func testKVs(t *testing.T, testFn func(t *testing.T, client kv.Client, reg *prometheus.Registry)) {
	setupFns := map[string]func(t *testing.T, scenario *e2e.Scenario, reg prometheus.Registerer, logger log.Logger) kv.Client{
		"etcd":   setupEtcd,
		"consul": setupConsul,
	}

	for name, setupFn := range setupFns {
		t.Run(name, func(t *testing.T) {
			testKVScenario(t, setupFn, testFn)
		})
	}
}

func testKVScenario(t *testing.T, kvSetupFn func(t *testing.T, scenario *e2e.Scenario, reg prometheus.Registerer, logger log.Logger) kv.Client, testFn func(t *testing.T, client kv.Client, reg *prometheus.Registry)) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	reg := prometheus.NewRegistry()
	client := kvSetupFn(t, s, prometheus.WrapRegistererWithPrefix("cortex_", reg), log.NewNopLogger())
	testFn(t, client, reg)
}

func verifyClientMetricsHistogram(t *testing.T, reg *prometheus.Registry, metricNameToVerify string, sampleCounts map[string]uint64) {
	metrics, err := reg.Gather()
	require.NoError(t, err)

	var metricToVerify *dto.MetricFamily
	for _, metric := range metrics {
		if metric.GetName() != metricNameToVerify {
			continue
		}
		metricToVerify = metric
		break
	}
	require.NotNilf(t, metricToVerify, "Metric %s not found in registry", metricNameToVerify)
	require.Equal(t, dto.MetricType_HISTOGRAM, metricToVerify.GetType())

	getMetricOperation := func(labels []*dto.LabelPair) (string, error) {
		for _, l := range labels {
			if l.GetName() == "operation" {
				return l.GetValue(), nil
			}
		}
		return "", errors.New("no operation")
	}

	for _, metric := range metricToVerify.GetMetric() {
		op, err := getMetricOperation(metric.Label)
		require.NoErrorf(t, err, "No operation label found in metric %v", metric.String())
		assert.Equal(t, sampleCounts[op], metric.GetHistogram().GetSampleCount(), op)
	}
}

type stringCodec struct{}

func (c stringCodec) Decode(bb []byte) (interface{}, error) {
	if bb == nil {
		return "<nil>", nil
	}
	return string(bb), nil
}
func (c stringCodec) Encode(v interface{}) ([]byte, error) { return []byte(v.(string)), nil }
func (c stringCodec) CodecID() string                      { return "stringCodec" }

type watcher struct {
	values map[string][]interface{}
}

func (w *watcher) watch(ctx context.Context, client kv.Client) {
	w.values = map[string][]interface{}{}
	client.WatchPrefix(ctx, "", func(key string, value interface{}) bool {
		w.values[key] = append(w.values[key], value)
		return true
	})
}
