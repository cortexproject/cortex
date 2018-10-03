package alertmanager

import (
	"net"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
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
	go disco.loop()
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
		level.Warn(util.Logger).Log("msg", "error discovering services for hostname", "service", s.service, "hostname", s.hostname, "err", err)
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
