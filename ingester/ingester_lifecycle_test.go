package ingester

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/common/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/cortex/ring"
)

func defaultIngesterTestConfig() Config {
	consul := ring.NewMockConsulClient()
	return Config{
		ringConfig: ring.Config{
			ConsulConfig: ring.ConsulConfig{
				Mock: consul,
			},
		},

		NumTokens:       1,
		HeartbeatPeriod: 5 * time.Second,
		ListenPort:      func(i int) *int { return &i }(0),
		addr:            "localhost",
		id:              "localhost",

		FlushCheckPeriod: 99999 * time.Hour,
		MaxChunkIdle:     99999 * time.Hour,
	}
}

// TestIngesterRestart tests a restarting ingester doesn't keep adding more tokens.
func TestIngesterRestart(t *testing.T) {
	config := defaultIngesterTestConfig()
	config.skipUnregister = true

	{
		ingester, err := New(config, nil)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		ingester.Shutdown() // doesn't actually unregister due to skipUnregister: true
	}

	poll(t, 100*time.Millisecond, 1, func() interface{} {
		return numTokens(config.ringConfig.ConsulConfig.Mock, "localhost")
	})

	{
		ingester, err := New(config, nil)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		ingester.Shutdown() // doesn't actually unregister due to skipUnregister: true
	}

	time.Sleep(200 * time.Millisecond)

	poll(t, 100*time.Millisecond, 1, func() interface{} {
		return numTokens(config.ringConfig.ConsulConfig.Mock, "localhost")
	})
}

func TestIngesterTransfer(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	store := &testStore{
		chunks: map[string][]chunk.Chunk{},
	}
	ing1, err := New(cfg, store)
	require.NoError(t, err)

	const userID = "1"

	var (
		ts  = model.TimeFromUnix(123)
		val = model.SampleValue(456)
	)

	ctx := user.Inject(context.Background(), userID)
	_, err = ing1.Push(ctx, util.ToWriteRequest([]model.Sample{
		{
			Metric: model.Metric{
				model.MetricNameLabel: "foo",
			},
			Timestamp: ts,
			Value:     val,
		},
	}))
	require.NoError(t, err)

}

func numTokens(c ring.ConsulClient, name string) int {
	ringDesc, err := c.Get(ring.ConsulKey)
	if err != nil {
		log.Errorf("Error reading consul: %v", err)
		return 0
	}

	count := 0
	for _, token := range ringDesc.(*ring.Desc).Tokens {
		if token.Ingester == name {
			count++
		}
	}
	return count
}

// poll repeatedly evaluates condition until we either timeout, or it succeeds.
func poll(t *testing.T, d time.Duration, want interface{}, have func() interface{}) {
	deadline := time.Now().Add(d)
	for {
		if time.Now().After(deadline) {
			break
		}
		if reflect.DeepEqual(want, have()) {
			return
		}
		time.Sleep(d / 10)
	}
	h := have()
	if !reflect.DeepEqual(want, h) {
		_, file, line, _ := runtime.Caller(1)
		t.Fatalf("%s:%d: %v != %v", file, line, want, h)
	}
}
