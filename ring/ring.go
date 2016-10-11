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

// Based on https://raw.githubusercontent.com/stathat/consistent/master/consistent.go

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
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
	name       string
	client     CoordinationStateClient
	quit, done chan struct{}

	mtx      sync.RWMutex
	ringDesc Desc

	ingesterOwnershipDesc *prometheus.Desc
	ingesterTotalDesc     *prometheus.Desc
	tokensTotalDesc       *prometheus.Desc
}

// New creates a new Ring
func New(name string, client CoordinationStateClient) *Ring {
	r := &Ring{
		name:   name,
		client: client,
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
		ingesterOwnershipDesc: prometheus.NewDesc(
			"prometheus_distributor_ingester_ownership_percent",
			"The percent ownership of the ring by ingester",
			[]string{"ingester"}, map[string]string{
				"ring": name,
			},
		),
		ingesterTotalDesc: prometheus.NewDesc(
			"prometheus_distributor_ingesters_total",
			"Number of ingesters in the ring",
			[]string{"ring"}, map[string]string{
				"ring": name,
			},
		),
		tokensTotalDesc: prometheus.NewDesc(
			"prometheus_distributor_tokens_total",
			"Number of tokens in the ring",
			[]string{"ring"}, map[string]string{
				"ring": name,
			},
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

// Get returns a collector close to the hash in the circle.
func (r *Ring) Get(key uint32) (IngesterDesc, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	if len(r.ringDesc.Tokens) == 0 {
		return IngesterDesc{}, ErrEmptyRing
	}
	i := r.search(key)
	return r.ringDesc.Ingesters[r.ringDesc.Tokens[i].Ingester], nil
}

// GetAll returns all ingesters in the circle.
func (r *Ring) GetAll() []IngesterDesc {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	ingesters := make([]IngesterDesc, 0, len(r.ringDesc.Ingesters))
	for _, ingester := range r.ringDesc.Ingesters {
		ingesters = append(ingesters, ingester)
	}
	return ingesters
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
	ch <- r.ingesterTotalDesc
	ch <- r.tokensTotalDesc
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

	ch <- prometheus.MustNewConstMetric(
		r.ingesterTotalDesc,
		prometheus.GaugeValue,
		float64(len(r.ringDesc.Ingesters)),
	)
	ch <- prometheus.MustNewConstMetric(
		r.tokensTotalDesc,
		prometheus.GaugeValue,
		float64(len(r.ringDesc.Tokens)),
	)
}
