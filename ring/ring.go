package ring

// Based on https://raw.githubusercontent.com/stathat/consistent/master/consistent.go

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

const (
	healthyLabel   = "healthy"
	unhealthyLabel = "unhealthy"
)

// Operation can be Read or Write
type Operation int

// Values for Operation
const (
	Read Operation = iota
	Write
)

type uint32s []uint32

func (x uint32s) Len() int           { return len(x) }
func (x uint32s) Less(i, j int) bool { return x[i] < x[j] }
func (x uint32s) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// ErrEmptyRing is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyRing = errors.New("empty circle")

// CoordinationStateClient is an interface to getting changes to the coordination
// state.  Should allow us to swap out Consul for something else (mesh?) later.
type CoordinationStateClient interface {
	WatchKey(key string, factory InstanceFactory, done <-chan struct{}, f func(interface{}) bool)
}

// Ring holds the information about the members of the consistent hash circle.
type Ring struct {
	client           CoordinationStateClient
	quit, done       chan struct{}
	heartbeatTimeout time.Duration

	mtx      sync.RWMutex
	ringDesc Desc

	ingesterOwnershipDesc *prometheus.Desc
	numIngestersDesc      *prometheus.Desc
	numTokensDesc         *prometheus.Desc
}

// New creates a new Ring
func New(client CoordinationStateClient, heartbeatTimeout time.Duration) *Ring {
	r := &Ring{
		client:           client,
		heartbeatTimeout: heartbeatTimeout,
		quit:             make(chan struct{}),
		done:             make(chan struct{}),
		ingesterOwnershipDesc: prometheus.NewDesc(
			"cortex_ring_ingester_ownership_percent",
			"The percent ownership of the ring by ingester",
			[]string{"ingester"}, nil,
		),
		numIngestersDesc: prometheus.NewDesc(
			"cortex_ring_ingesters",
			"Number of ingesters in the ring",
			[]string{"state"}, nil,
		),
		numTokensDesc: prometheus.NewDesc(
			"cortex_ring_tokens",
			"Number of tokens in the ring",
			nil, nil,
		),
	}
	go r.loop()
	return r
}

// Stop the distributor.
func (r *Ring) Stop() {
	close(r.quit)
	<-r.done
}

func (r *Ring) loop() {
	defer close(r.done)
	r.client.WatchKey(consulKey, descFactory, r.quit, func(value interface{}) bool {
		if value == nil {
			log.Infof("Ring doesn't exist in consul yet.")
			return true
		}

		ringDesc := value.(*Desc)
		r.mtx.Lock()
		defer r.mtx.Unlock()
		r.ringDesc = *ringDesc
		return true
	})
}

// Get returns n (or more) ingesters which form the replicas for the given key.
func (r *Ring) Get(key uint32, n int, op Operation) ([]IngesterDesc, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return r.getInternal(key, n, op)
}

// BatchGet returns n (or more) ingesters which form the replicas for the given key.
// The order of the result matches the order of the input.
func (r *Ring) BatchGet(keys []uint32, n int, op Operation) ([][]IngesterDesc, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	result := make([][]IngesterDesc, len(keys), len(keys))
	for i, key := range keys {
		ingesters, err := r.getInternal(key, n, op)
		if err != nil {
			return nil, err
		}
		result[i] = ingesters
	}
	return result, nil
}

func (r *Ring) getInternal(key uint32, n int, op Operation) ([]IngesterDesc, error) {
	if len(r.ringDesc.Tokens) == 0 {
		return nil, ErrEmptyRing
	}

	ingesters := make([]IngesterDesc, 0, n)
	distinctHosts := map[string]struct{}{}
	start := r.search(key)
	iterations := 0
	for i := start; len(distinctHosts) < n && iterations < len(r.ringDesc.Tokens); i++ {
		iterations++

		// Wrap i around in the ring.
		j := i % len(r.ringDesc.Tokens)

		// We want n *distinct* ingesters.
		token := r.ringDesc.Tokens[j]
		if _, ok := distinctHosts[token.Ingester]; ok {
			continue
		}
		distinctHosts[token.Ingester] = struct{}{}

		// Ingesters that are Leaving do not count to the replication limit. We do
		// not want to Write to them because they are about to go away, but we do
		// want to write the extra replica somewhere.  So we increase the size of the
		// set of replicas for the key.  This means we have to also increase the
		// size of the replica set for read, but we can read from Leaving ingesters,
		// so don't skip it in this case.
		if token.State == Leaving {
			n++
			if op == Write {
				continue
			}
		}

		ing := r.ringDesc.Ingesters[token.Ingester]
		ingesters = append(ingesters, ing)
	}
	return ingesters, nil
}

// GetAll returns all available ingesters in the circle.
func (r *Ring) GetAll() []IngesterDesc {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	ingesters := make([]IngesterDesc, 0, len(r.ringDesc.Ingesters))
	for _, ingester := range r.ringDesc.Ingesters {
		if time.Now().Sub(ingester.Timestamp) > r.heartbeatTimeout {
			continue
		}
		ingesters = append(ingesters, ingester)
	}
	return ingesters
}

// Ready is true when all ingesters are active and healthy.
func (r *Ring) Ready() bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	for _, ingester := range r.ringDesc.Ingesters {
		if time.Now().Sub(ingester.Timestamp) > r.heartbeatTimeout {
			return false
		}
	}

	for _, token := range r.ringDesc.Tokens {
		if token.State != Active {
			return false
		}
	}

	return len(r.ringDesc.Tokens) > 0
}

func (r *Ring) search(key uint32) int {
	i := sort.Search(len(r.ringDesc.Tokens), func(x int) bool {
		return r.ringDesc.Tokens[x].Token > key
	})
	if i >= len(r.ringDesc.Tokens) {
		i = 0
	}
	return i
}

// Describe implements prometheus.Collector.
func (r *Ring) Describe(ch chan<- *prometheus.Desc) {
	ch <- r.ingesterOwnershipDesc
	ch <- r.numIngestersDesc
	ch <- r.numTokensDesc
}

// Collect implements prometheus.Collector.
func (r *Ring) Collect(ch chan<- prometheus.Metric) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	owned := map[string]uint32{}
	for i, token := range r.ringDesc.Tokens {
		var diff uint32
		if i+1 == len(r.ringDesc.Tokens) {
			diff = (math.MaxUint32 - token.Token) + r.ringDesc.Tokens[0].Token
		} else {
			diff = r.ringDesc.Tokens[i+1].Token - token.Token
		}
		owned[token.Ingester] = owned[token.Ingester] + diff
	}

	for id, totalOwned := range owned {
		ch <- prometheus.MustNewConstMetric(
			r.ingesterOwnershipDesc,
			prometheus.GaugeValue,
			float64(totalOwned)/float64(math.MaxUint32),
			id,
		)
	}

	healthy, unhealthy := 0, 0
	for _, ingester := range r.ringDesc.Ingesters {
		if time.Now().Sub(ingester.Timestamp) > r.heartbeatTimeout {
			unhealthy++
		} else {
			healthy++
		}
	}

	ch <- prometheus.MustNewConstMetric(
		r.numIngestersDesc,
		prometheus.GaugeValue,
		float64(healthy),
		healthyLabel,
	)
	ch <- prometheus.MustNewConstMetric(
		r.numIngestersDesc,
		prometheus.GaugeValue,
		float64(unhealthy),
		unhealthyLabel,
	)
	ch <- prometheus.MustNewConstMetric(
		r.numTokensDesc,
		prometheus.GaugeValue,
		float64(len(r.ringDesc.Tokens)),
	)
}
