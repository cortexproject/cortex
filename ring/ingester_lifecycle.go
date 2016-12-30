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
	consul         ConsulClient
	numTokens      int
	skipUnregister bool

	id           string
	hostname     string
	grpcHostname string
	quit         chan struct{}
	wait         sync.WaitGroup

	// We need to remember the ingester state just in case consul goes away and comes
	// back empty.  Channel is used to tell the actor to update consul on state changes.
	state       IngesterState
	stateChange chan IngesterState

	consulHeartbeats prometheus.Counter
}

type IngesterRegistrationConfig struct {
	ListenPort int
	GRPCPort   int
	NumTokens  int

	// For testing
	Addr           string
	Hostname       string
	skipUnregister bool
}

// RegisterIngester registers an ingester with Consul.
func RegisterIngester(consulClient ConsulClient, cfg IngesterRegistrationConfig) (*IngesterRegistration, error) {
	hostname := cfg.Hostname
	if hostname == "" {
		var err error
		hostname, err = os.Hostname()
		if err != nil {
			return nil, err
		}
	}

	addr := cfg.Addr
	if addr == "" {
		var err error
		addr, err = getFirstAddressOf(infName)
		if err != nil {
			return nil, err
		}
	}

	r := &IngesterRegistration{
		consul:         consulClient,
		numTokens:      cfg.NumTokens,
		skipUnregister: cfg.skipUnregister,

		id: hostname,
		// hostname is the ip+port of this instance, written to consul so
		// the distributors know where to connect.
		hostname:     fmt.Sprintf("%s:%d", addr, cfg.ListenPort),
		grpcHostname: fmt.Sprintf("%s:%d", addr, cfg.GRPCPort),
		quit:         make(chan struct{}),

		// Only read/written on actor goroutine.
		state:       Active,
		stateChange: make(chan IngesterState),

		consulHeartbeats: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_consul_heartbeats_total",
			Help: "The total number of heartbeats sent to consul.",
		}),
	}

	r.wait.Add(1)
	go r.loop()
	return r, nil
}

// ChangeState changes the state of an ingester in the ring.
func (r *IngesterRegistration) ChangeState(state IngesterState) {
	log.Info("Changing ingester state to: %v", state)
	r.stateChange <- state
}

// Unregister removes ingester config from Consul; will block
// until we'll successfully unregistered.
func (r *IngesterRegistration) Unregister() {
	// closing r.quit triggers loop() to exit, which in turn will trigger
	// the removal of our tokens.
	close(r.quit)
	r.wait.Wait()
}

func (r *IngesterRegistration) loop() {
	defer r.wait.Done()
	tokens := r.pickTokens()

	if !r.skipUnregister {
		defer r.unregister()
	}

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

		var takenTokens, myTokens []uint32
		for _, token := range ringDesc.Tokens {
			takenTokens = append(takenTokens, token.Token)

			if token.Ingester == r.id {
				myTokens = append(myTokens, token.Token)
			}
		}

		if len(myTokens) > 0 {
			log.Infof("%d tokens already exist for this ingester!", len(myTokens))
		}

		newTokens := generateTokens(r.numTokens-len(myTokens), takenTokens)
		ringDesc.addIngester(r.id, r.hostname, r.grpcHostname, newTokens, r.state)

		tokens := append(myTokens, newTokens...)
		sort.Sort(sortableUint32(tokens))

		return ringDesc, true, nil
	}
	if err := r.consul.CAS(consulKey, descFactory, pickTokens); err != nil {
		log.Fatalf("Failed to pick tokens in consul: %v", err)
		return nil
	}
	log.Infof("Ingester added to consul")
	return tokens
}

func (r *IngesterRegistration) heartbeat(tokens []uint32) {
	updateConsul := func(in interface{}) (out interface{}, retry bool, err error) {
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
			ringDesc.addIngester(r.id, r.hostname, r.grpcHostname, tokens, r.state)
		} else {
			ingesterDesc.Timestamp = time.Now()
			ingesterDesc.State = r.state
			ringDesc.Ingesters[r.id] = ingesterDesc
		}

		return ringDesc, true, nil
	}

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case r.state = <-r.stateChange:
			if err := r.consul.CAS(consulKey, descFactory, updateConsul); err != nil {
				log.Errorf("Failed to write to consul, sleeping: %v", err)
			}
		case <-ticker.C:
			r.consulHeartbeats.Inc()
			if err := r.consul.CAS(consulKey, descFactory, updateConsul); err != nil {
				log.Errorf("Failed to write to consul, sleeping: %v", err)
			}
		case <-r.quit:
			return
		}
	}
}

func (r *IngesterRegistration) unregister() {
	unregister := func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*Desc)
		ringDesc.removeIngester(r.id)
		return ringDesc, true, nil
	}
	if err := r.consul.CAS(consulKey, descFactory, unregister); err != nil {
		log.Fatalf("Failed to unregister from consul: %v", err)
	}
	log.Infof("Ingester removed from consul")
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
