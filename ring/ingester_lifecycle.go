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

// Responsible for managing the ingester lifecycle.

package ring

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

const (
	infName           = "eth0"
	consulKey         = "ring"
	heartbeatInterval = 5 * time.Second
)

// IngesterRegistration manages the connection between the ingester and Consul.
type IngesterRegistration struct {
	consul    ConsulClient
	numTokens int

	id       string
	hostname string
	quit     chan struct{}
	wait     sync.WaitGroup

	consulHeartbeats prometheus.Counter
}

// RegisterIngester registers an ingester with Consul.
func RegisterIngester(name string, consulClient ConsulClient, listenPort, numTokens int) (*IngesterRegistration, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	addr, err := getFirstAddressOf(infName)
	if err != nil {
		return nil, err
	}

	r := &IngesterRegistration{
		consul:    consulClient,
		numTokens: numTokens,

		id: hostname,
		// hostname is the ip+port of this instance, written to consul so
		// the distributors know where to connect.
		hostname: fmt.Sprintf("%s:%d", addr, listenPort),
		quit:     make(chan struct{}),

		consulHeartbeats: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "prism_ingester_consul_heartbeats_total",
			Help:        "The total number of heartbeats sent to consul.",
			ConstLabels: map[string]string{"ring": name},
		}),
	}

	r.wait.Add(1)
	go r.loop()
	return r, nil
}

// Unregister removes ingester config from Consul; will block
// until we'll successfully unregistered.
func (r *IngesterRegistration) Unregister() {
	log.Info("Removing ingester from consul")

	// closing r.quit triggers loop() to exit, which in turn will trigger
	// the removal of our tokens.
	close(r.quit)
	r.wait.Wait()
	log.Infof("Ingester removed from consul")
}

func (r *IngesterRegistration) loop() {
	defer r.wait.Done()
	tokens := r.pickTokens()
	defer r.unregister(tokens)
	r.heartbeat(tokens)
}

func (r *IngesterRegistration) pickTokens() []uint32 {
	var tokens []uint32
	pickTokens := func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *Desc
		if in == nil {
			ringDesc = newDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		takenTokens := []uint32{}
		for _, token := range ringDesc.Tokens {
			takenTokens = append(takenTokens, token.Token)
			if token.Ingester == r.id {
				tokens = append(tokens, token.Token)
			}
		}
		if len(tokens) < r.numTokens {
			newTokens := generateTokens(r.numTokens-len(tokens), takenTokens)
			tokens = append(tokens, newTokens...)
		}

		ringDesc.addIngester(r.id, r.hostname, tokens)
		return ringDesc, true, nil
	}
	if err := r.consul.CAS(consulKey, descFactory, pickTokens); err != nil {
		log.Fatalf("Failed to pick tokens in consul: %v", err)
	}
	return tokens
}

func (r *IngesterRegistration) heartbeat(tokens []uint32) {
	heartbeat := func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *Desc
		if in == nil {
			ringDesc = newDesc()
		} else {
			ringDesc = in.(*Desc)
		}

		ingesterDesc, ok := ringDesc.Ingesters[r.id]
		if !ok {
			// consul must have restarted
			log.Infof("Found empty ring, inserting tokens!")
			ringDesc.addIngester(r.id, r.hostname, tokens)
		} else {
			ingesterDesc.Timestamp = time.Now()
			ringDesc.Ingesters[r.id] = ingesterDesc
		}

		return ringDesc, true, nil
	}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.consulHeartbeats.Inc()
			if err := r.consul.CAS(consulKey, descFactory, heartbeat); err != nil {
				log.Errorf("Failed to write to consul, sleeping: %v", err)
			}
		case <-r.quit:
			return
		}
	}
}

func (r *IngesterRegistration) unregister(tokens []uint32) {
	unregister := func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*Desc)
		ringDesc.removeIngester(r.id, tokens)
		return ringDesc, true, nil
	}
	if err := r.consul.CAS(consulKey, descFactory, unregister); err != nil {
		log.Fatalf("Failed to unregister from consul: %v", err)
	}
}

type sortableUint32 []uint32

func (ts sortableUint32) Len() int           { return len(ts) }
func (ts sortableUint32) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }
func (ts sortableUint32) Less(i, j int) bool { return ts[i] < ts[j] }

// generateTokens make numTokens random tokens, none of which clash
// with takenTokens.  Assumes takenTokens is sorted.
func generateTokens(numTokens int, takenTokens []uint32) []uint32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokens := sortableUint32{}
	for i := 0; i < numTokens; {
		candidate := r.Uint32()
		j := sort.Search(len(takenTokens), func(i int) bool {
			return takenTokens[i] >= candidate
		})
		if j < len(takenTokens) && takenTokens[j] == candidate {
			continue
		}
		tokens = append(tokens, candidate)
		i++
	}
	sort.Sort(tokens)
	return tokens
}

// getFirstAddressOf returns the first IPv4 address of the supplied interface name.
func getFirstAddressOf(name string) (string, error) {
	inf, err := net.InterfaceByName(name)
	if err != nil {
		return "", err
	}

	addrs, err := inf.Addrs()
	if err != nil {
		return "", err
	}
	if len(addrs) <= 0 {
		return "", fmt.Errorf("No address found for %s", name)
	}

	for _, addr := range addrs {
		switch v := addr.(type) {
		case *net.IPNet:
			if ip := v.IP.To4(); ip != nil {
				return v.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("No address found for %s", name)
}

// Describe implements prometheus.Collector.
func (r *IngesterRegistration) Describe(ch chan<- *prometheus.Desc) {
	ch <- r.consulHeartbeats.Desc()
}

// Collect implements prometheus.Collector.
func (r *IngesterRegistration) Collect(ch chan<- prometheus.Metric) {
	ch <- r.consulHeartbeats
}
