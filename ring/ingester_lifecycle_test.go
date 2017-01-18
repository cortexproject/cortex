package ring

import (
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestIngesterRestart(t *testing.T) {
	consul := newMockConsulClient()
	ring, err := New(Config{
		ConsulConfig: ConsulConfig{
			mock: consul,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	{
		registra, err := RegisterIngester(IngesterRegistrationConfig{
			mock:           ring,
			skipUnregister: true,

			NumTokens:  1,
			ListenPort: func(i int) *int { return &i }(0),
			Addr:       "localhost",
			Hostname:   "localhost",
		})
		if err != nil {
			t.Fatal(err)
		}
		registra.Unregister() // doesn't actually unregister due to skipUnregister: true
	}

	poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ring.numTokens("localhost")
	})

	{
		registra, err := RegisterIngester(IngesterRegistrationConfig{
			mock:           ring,
			skipUnregister: true,

			NumTokens:  1,
			ListenPort: func(i int) *int { return &i }(0),
			Addr:       "localhost",
			Hostname:   "localhost",
		})
		if err != nil {
			t.Fatal(err)
		}
		registra.Unregister() // doesn't actually unregister due to skipUnregister: true
	}

	time.Sleep(200 * time.Millisecond)

	poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ring.numTokens("localhost")
	})
}

func (r *Ring) numTokens(name string) int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	count := 0
	for _, token := range r.ringDesc.Tokens {
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
