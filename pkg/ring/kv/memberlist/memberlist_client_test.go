package memberlist

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
)

const ACTIVE = 1
const JOINING = 2
const LEFT = 3

// Simple mergeable data structure, used for gossiping
type member struct {
	Timestamp int64
	Tokens    []uint32
	State     int
}

type data struct {
	Members map[string]member
}

func (d *data) Merge(mergeable Mergeable, localCAS bool) (Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	od, ok := mergeable.(*data)
	if !ok || od == nil {
		return nil, fmt.Errorf("invalid thing to merge: %T", od)
	}

	updated := map[string]member{}

	for k, v := range od.Members {
		if v.Timestamp > d.Members[k].Timestamp {
			d.Members[k] = v
			updated[k] = v
		}
	}

	if localCAS {
		for k, v := range d.Members {
			if _, ok := od.Members[k]; !ok && v.State != LEFT {
				v.State = LEFT
				v.Tokens = nil
				d.Members[k] = v
				updated[k] = v
			}
		}
	}

	if len(updated) == 0 {
		return nil, nil
	}
	return &data{Members: updated}, nil
}

func (d *data) MergeContent() []string {
	// return list of keys
	out := []string(nil)
	for k := range d.Members {
		out = append(out, k)
	}
	return out
}

func (d *data) RemoveTombstones(limit time.Time) {
	// nothing to do
}

func (d *data) getAllTokens() []uint32 {
	out := []uint32(nil)
	for _, m := range d.Members {
		out = append(out, m.Tokens...)
	}

	sort.Sort(sortableUint32(out))
	return out
}

type dataCodec struct{}

func (d dataCodec) Decode(b []byte) (interface{}, error) {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	out := &data{}
	err := dec.Decode(out)
	return out, err
}

func (d dataCodec) Encode(val interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	return buf.Bytes(), err
}

var _ codec.Codec = &dataCodec{}

type sortableUint32 []uint32

func (ts sortableUint32) Len() int           { return len(ts) }
func (ts sortableUint32) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts sortableUint32) Less(i, j int) bool { return ts[i] < ts[j] }

const key = "test"

func updateFn(name string) func(*data) (*data, bool, error) {
	return func(in *data) (out *data, retry bool, err error) {
		// Modify value that was passed as a parameter.
		// Client takes care of concurrent modifications.
		var r *data = in
		if r == nil {
			r = &data{Members: map[string]member{}}
		}

		m, ok := r.Members[name]
		if !ok {
			r.Members[name] = member{
				Timestamp: time.Now().Unix(),
				Tokens:    generateTokens(128),
				State:     JOINING,
			}
		} else {
			// We need to update timestamp, otherwise CAS will fail
			m.Timestamp = time.Now().Unix()
			m.State = ACTIVE
			r.Members[name] = m
		}

		return r, true, nil
	}
}

func get(t *testing.T, kv *Client, key string) interface{} {
	val, err := kv.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to get value for key %s: %v", key, err)
	}
	return val
}

func getData(t *testing.T, kv *Client, key string) *data {
	t.Helper()
	val := get(t, kv, key)
	if val == nil {
		return nil
	}
	if r, ok := val.(*data); ok {
		return r
	}
	t.Fatalf("Expected ring, got: %T", val)
	return nil
}

func cas(t *testing.T, kv *Client, key string, updateFn func(*data) (*data, bool, error)) {
	t.Helper()

	if err := casWithErr(context.Background(), t, kv, key, updateFn); err != nil {
		t.Fatal(err)
	}
}

func casWithErr(ctx context.Context, t *testing.T, kv *Client, key string, updateFn func(*data) (*data, bool, error)) error {
	t.Helper()
	fn := func(in interface{}) (out interface{}, retry bool, err error) {
		var r *data = nil
		if in != nil {
			r = in.(*data)
		}

		d, rt, e := updateFn(r)
		if d == nil {
			// translate nil pointer to nil interface value
			return nil, rt, e
		}
		return d, rt, e
	}

	return kv.CAS(ctx, key, fn)
}

func TestBasicGetAndCas(t *testing.T) {
	c := dataCodec{}

	name := "Ing 1"
	cfg := Config{
		TCPTransport: TCPTransportConfig{
			BindAddrs: []string{"localhost"},
		},
	}

	kv, err := NewMemberlistClient(cfg, c)
	if err != nil {
		t.Fatal("Failed to setup KV client", err)
	}
	defer kv.Stop()

	const key = "test"

	val := get(t, kv, key)
	if val != nil {
		t.Error("Expected nil, got:", val)
	}

	// Create member in PENDING state, with some tokens
	cas(t, kv, key, updateFn(name))

	r := getData(t, kv, key)
	if r == nil || r.Members[name].Timestamp == 0 || len(r.Members[name].Tokens) <= 0 {
		t.Fatalf("Expected ring with tokens, got %v", r)
	}

	val = get(t, kv, "other key")
	if val != nil {
		t.Errorf("Expected nil, got: %v", val)
	}

	// Update member into ACTIVE state
	cas(t, kv, key, updateFn(name))
	r = getData(t, kv, key)
	if r.Members[name].State != ACTIVE {
		t.Errorf("Expected member to be active after second update, got %v", r)
	}

	// Delete member
	cas(t, kv, key, func(r *data) (*data, bool, error) {
		delete(r.Members, name)
		return r, true, nil
	})

	r = getData(t, kv, key)
	if r.Members[name].State != LEFT {
		t.Errorf("Expected member to be LEFT, got %v", r)
	}
}

func withFixtures(t *testing.T, testFN func(t *testing.T, kv *Client)) {
	t.Helper()

	c := dataCodec{}

	cfg := Config{
		TCPTransport: TCPTransportConfig{},
	}

	kv, err := NewMemberlistClient(cfg, c)
	if err != nil {
		t.Fatal("Failed to setup KV client", err)
	}
	defer kv.Stop()

	testFN(t, kv)
}

func TestCASNoOutput(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		// should succeed with single call
		calls := 0
		cas(t, kv, key, func(d *data) (*data, bool, error) {
			calls++
			return nil, true, nil
		})

		require.Equal(t, 1, calls)
	})
}

func TestCASErrorNoRetry(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		calls := 0
		err := casWithErr(context.Background(), t, kv, key, func(d *data) (*data, bool, error) {
			calls++
			return nil, false, errors.New("don't worry, be happy")
		})
		require.EqualError(t, err, "failed to CAS-update key test: fn returned error: don't worry, be happy")
		require.Equal(t, 1, calls)
	})
}

func TestCASErrorWithRetries(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		calls := 0
		err := casWithErr(context.Background(), t, kv, key, func(d *data) (*data, bool, error) {
			calls++
			return nil, true, errors.New("don't worry, be happy")
		})
		require.EqualError(t, err, "failed to CAS-update key test: fn returned error: don't worry, be happy")
		require.Equal(t, 10, calls) // hard-coded in CAS function.
	})
}

func TestCASNoChange(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		cas(t, kv, key, func(in *data) (*data, bool, error) {
			if in == nil {
				in = &data{Members: map[string]member{}}
			}

			in.Members["hello"] = member{
				Timestamp: time.Now().Unix(),
				Tokens:    generateTokens(128),
				State:     JOINING,
			}

			return in, true, nil
		})

		startTime := time.Now()
		calls := 0
		err := casWithErr(context.Background(), t, kv, key, func(d *data) (*data, bool, error) {
			calls++
			return d, true, nil
		})
		require.EqualError(t, err, "failed to CAS-update key test: no change detected")
		require.Equal(t, maxCasRetries, calls)
		// if there was no change, CAS sleeps before every retry
		require.True(t, time.Since(startTime) >= (maxCasRetries-1)*noChangeDetectedRetrySleep)
	})
}

func TestCASNoChangeShortTimeout(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		cas(t, kv, key, func(in *data) (*data, bool, error) {
			if in == nil {
				in = &data{Members: map[string]member{}}
			}

			in.Members["hello"] = member{
				Timestamp: time.Now().Unix(),
				Tokens:    generateTokens(128),
				State:     JOINING,
			}

			return in, true, nil
		})

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		calls := 0
		err := casWithErr(ctx, t, kv, key, func(d *data) (*data, bool, error) {
			calls++
			return d, true, nil
		})
		require.EqualError(t, err, "failed to CAS-update key test: context deadline exceeded")
		require.Equal(t, 1, calls) // hard-coded in CAS function.
	})
}

func TestCASFailedBecauseOfVersionChanges(t *testing.T) {
	withFixtures(t, func(t *testing.T, kv *Client) {
		cas(t, kv, key, func(in *data) (*data, bool, error) {
			return &data{Members: map[string]member{"nonempty": {Timestamp: time.Now().Unix()}}}, true, nil
		})

		calls := 0
		// outer cas
		err := casWithErr(context.Background(), t, kv, key, func(d *data) (*data, bool, error) {
			// outer CAS logic
			calls++

			// run inner-CAS that succeeds, and that will make outer cas to fail
			cas(t, kv, key, func(d *data) (*data, bool, error) {
				// to avoid delays due to merging, we update different ingester each time.
				d.Members[fmt.Sprintf("%d", calls)] = member{
					Timestamp: time.Now().Unix(),
				}
				return d, true, nil
			})

			d.Members["world"] = member{
				Timestamp: time.Now().Unix(),
			}
			return d, true, nil
		})

		require.EqualError(t, err, "failed to CAS-update key test: too many retries")
		require.Equal(t, maxCasRetries, calls)
	})
}

func TestMultipleCAS(t *testing.T) {
	c := dataCodec{}

	cfg := Config{}

	kv, err := NewMemberlistClient(cfg, c)
	if err != nil {
		t.Fatal("Failed to setup KV client", err)
	}
	defer kv.Stop()

	wg := &sync.WaitGroup{}
	start := make(chan struct{})

	const members = 10
	const namePattern = "Member-%d"

	for i := 0; i < members; i++ {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			<-start
			up := updateFn(name)
			cas(t, kv, "test", up) // PENDING state
			cas(t, kv, "test", up) // ACTIVE state
		}(fmt.Sprintf(namePattern, i))
	}

	close(start) // start all CAS updates
	wg.Wait()    // wait until all CAS updates are finished

	// Now let's test that all members are in ACTIVE state
	r := getData(t, kv, "test")
	require.True(t, r != nil, "nil ring")

	for i := 0; i < members; i++ {
		n := fmt.Sprintf(namePattern, i)

		if r.Members[n].State != ACTIVE {
			t.Errorf("Expected member %s to be ACTIVE got %v", n, r.Members[n].State)
		}
	}

	// Make all members leave
	start = make(chan struct{})

	for i := 0; i < members; i++ {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()

			<-start
			up := func(in *data) (out *data, retry bool, err error) {
				delete(in.Members, name)
				return in, true, nil
			}
			cas(t, kv, "test", up) // PENDING state
		}(fmt.Sprintf(namePattern, i))
	}

	close(start) // start all CAS updates
	wg.Wait()    // wait until all CAS updates are finished

	r = getData(t, kv, "test")
	require.True(t, r != nil, "nil ring")

	for i := 0; i < members; i++ {
		n := fmt.Sprintf(namePattern, i)

		if r.Members[n].State != LEFT {
			t.Errorf("Expected member %s to be ACTIVE got %v", n, r.Members[n].State)
		}
	}
}

func TestMultipleClients(t *testing.T) {
	c := dataCodec{}
	l := log.NewLogfmtLogger(os.Stdout)
	l = level.NewFilter(l, level.AllowInfo())

	const members = 10
	const key = "ring"

	var clients []*Client

	stop := make(chan struct{})
	start := make(chan struct{})

	port := 0

	for i := 0; i < members; i++ {
		id := fmt.Sprintf("Member-%d", i)
		cfg := Config{
			NodeName: id,

			// some useful parameters when playing with higher number of members
			// RetransmitMult:     2,
			GossipInterval:   100 * time.Millisecond,
			GossipNodes:      3,
			PushPullInterval: 5 * time.Second,
			// PacketDialTimeout:  5 * time.Second,
			// StreamTimeout:      5 * time.Second,
			// PacketWriteTimeout: 2 * time.Second,

			TCPTransport: TCPTransportConfig{
				BindAddrs: []string{"localhost"},
				BindPort:  0, // randomize ports
			},
		}

		kv, err := NewMemberlistClient(cfg, c)
		if err != nil {
			t.Fatal(id, err)
		}

		clients = append(clients, kv)

		go runClient(t, kv, id, key, port, start, stop)

		// next KV will connect to this one
		port = kv.GetListeningPort()
	}

	println("Waiting before start")
	time.Sleep(2 * time.Second)
	close(start)

	println("Observing ring ...")

	startTime := time.Now()
	firstKv := clients[0]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	updates := 0
	firstKv.WatchKey(ctx, key, func(in interface{}) bool {
		updates++

		r := in.(*data)

		minTimestamp, maxTimestamp, avgTimestamp := getTimestamps(r.Members)

		now := time.Now()
		t.Log("Update", now.Sub(startTime).String(), ": Ring has", len(r.Members), "members, and", len(r.getAllTokens()),
			"tokens, oldest timestamp:", now.Sub(time.Unix(minTimestamp, 0)).String(),
			"avg timestamp:", now.Sub(time.Unix(avgTimestamp, 0)).String(),
			"youngest timestamp:", now.Sub(time.Unix(maxTimestamp, 0)).String())
		return true // yes, keep watching
	})
	cancel() // make linter happy

	// Let clients exchange messages for a while
	close(stop)

	t.Logf("Ring updates observed: %d", updates)

	if updates < members {
		// in general, at least one update from each node. (although that's not necessarily true...
		// but typically we get more updates than that anyway)
		t.Errorf("expected to see updates, got %d", updates)
	}

	// Let's check all the clients to see if they have relatively up-to-date information
	// All of them should at least have all the clients
	// And same tokens.
	allTokens := []uint32(nil)

	for i := 0; i < members; i++ {
		kv := clients[i]

		r := getData(t, kv, key)
		t.Logf("KV %d: number of known members: %d\n", i, len(r.Members))
		if len(r.Members) != members {
			t.Errorf("Member %d has only %d members in the ring", i, len(r.Members))
		}

		minTimestamp, maxTimestamp, avgTimestamp := getTimestamps(r.Members)
		for n, ing := range r.Members {
			if ing.State != ACTIVE {
				t.Errorf("Member %d: invalid state of member %s in the ring: %v ", i, n, ing.State)
			}
		}
		now := time.Now()
		t.Logf("Member %d: oldest: %v, avg: %v, youngest: %v", i,
			now.Sub(time.Unix(minTimestamp, 0)).String(),
			now.Sub(time.Unix(avgTimestamp, 0)).String(),
			now.Sub(time.Unix(maxTimestamp, 0)).String())

		tokens := r.getAllTokens()
		if allTokens == nil {
			allTokens = tokens
			t.Logf("Found tokens: %d", len(allTokens))
		} else {
			if len(allTokens) != len(tokens) {
				t.Errorf("Member %d: Expected %d tokens, got %d", i, len(allTokens), len(tokens))
			} else {
				for ix, tok := range allTokens {
					if tok != tokens[ix] {
						t.Errorf("Member %d: Tokens at position %d differ: %v, %v", i, ix, tok, tokens[ix])
						break
					}
				}
			}
		}
	}
}

func getTimestamps(members map[string]member) (min int64, max int64, avg int64) {
	min = int64(math.MaxInt64)

	for _, ing := range members {
		if ing.Timestamp < min {
			min = ing.Timestamp
		}

		if ing.Timestamp > max {
			max = ing.Timestamp
		}

		avg += ing.Timestamp
	}
	if len(members) > 0 {
		avg /= int64(len(members))
	}
	return
}

func runClient(t *testing.T, kv *Client, name string, ringKey string, portToConnect int, start <-chan struct{}, stop <-chan struct{}) {
	// stop gossipping about the ring(s)
	defer kv.Stop()

	for {
		select {
		case <-start:
			start = nil

			// let's join the first member
			if portToConnect > 0 {
				_, err := kv.JoinMembers([]string{fmt.Sprintf("127.0.0.1:%d", portToConnect)})
				if err != nil {
					t.Fatalf("%s failed to join the cluster: %v", name, err)
					return
				}
			}
		case <-stop:
			return
		case <-time.After(1 * time.Second):
			cas(t, kv, ringKey, updateFn(name))
		}
	}
}

// avoid dependency on ring package
func generateTokens(numTokens int) []uint32 {
	var tokens []uint32
	for i := 0; i < numTokens; {
		candidate := rand.Uint32()
		tokens = append(tokens, candidate)
		i++
	}
	return tokens
}
