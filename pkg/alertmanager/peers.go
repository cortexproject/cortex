package alertmanager

import (
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

var (
	srvRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "srv_lookup_requests_total",
		Help:      "Total number of SRV requests.",
	})
	srvRequestFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "srv_lookup_request_failures_total",
		Help:      "Total number of failed SRV requests.",
	})
)

func init() {
	prometheus.MustRegister(srvRequests)
	prometheus.MustRegister(srvRequestFailures)
}

// TODO: change memcache_client to use this.

// srvDiscovery discovers SRV services.
type srvDiscovery struct {
	service      string
	hostname     string
	pollInterval time.Duration
	addresses    chan []*net.SRV

	stop chan struct{}
	done chan struct{}
}

// newSRVDiscovery makes a new srvDiscovery.
func newSRVDiscovery(service, hostname string, pollInterval time.Duration) *srvDiscovery {
	disco := &srvDiscovery{
		service:      service,
		hostname:     hostname,
		pollInterval: pollInterval,
		addresses:    make(chan []*net.SRV),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	if service != "" && hostname != "" {
		go disco.loop()
	} else {
		log.Infof("Peer discovery disabled")
	}
	return disco
}

// Stop the srvDiscovery
func (s *srvDiscovery) Stop() {
	close(s.stop)
	<-s.done
}

func (s *srvDiscovery) updatePeers() {
	var addrs []*net.SRV
	_, addrs, err := net.LookupSRV(s.service, "tcp", s.hostname)
	srvRequests.Inc()
	if err != nil {
		srvRequestFailures.Inc()
		log.Warnf("Error discovering services for %s %s: %v", s.service, s.hostname, err)
	} else {
		s.addresses <- addrs
	}
}

func (s *srvDiscovery) loop() {
	defer close(s.done)
	ticker := time.NewTicker(s.pollInterval)
	s.updatePeers()
	for {
		select {
		case <-ticker.C:
			s.updatePeers()
		case <-s.stop:
			ticker.Stop()
			return
		}
	}
}
