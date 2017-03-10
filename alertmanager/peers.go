package alertmanager

import (
	"net"
	"time"

	"github.com/prometheus/common/log"
)

// TODO: change memcache_client to use this.

// SRVDiscovery discovers SRV services.
type SRVDiscovery struct {
	Service      string
	Proto        string
	Hostname     string
	PollInterval time.Duration
	Addresses    chan []*net.SRV

	stop chan struct{}
	done chan struct{}
}

// NewSRVDiscovery makes a new SRVDiscovery.
func NewSRVDiscovery(service, hostname string, pollInterval time.Duration) *SRVDiscovery {
	disco := &SRVDiscovery{
		Service:      service,
		Proto:        "tcp",
		Hostname:     hostname,
		PollInterval: pollInterval,
		Addresses:    make(chan []*net.SRV),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	go disco.loop()
	return disco
}

// Stop the SRVDiscovery
func (s *SRVDiscovery) Stop() {
	close(s.stop)
	<-s.done
}

func (s *SRVDiscovery) loop() {
	defer close(s.done)
	ticker := time.NewTicker(s.PollInterval)
	for {
		select {
		case <-ticker.C:
			_, addrs, err := net.LookupSRV(s.Service, s.Proto, s.Hostname)
			if err != nil {
				log.Warnf("Error discovering services for %s %s %s: %v", s.Service, s.Proto, s.Hostname, err)
			}
			s.Addresses <- addrs
		case <-s.stop:
			ticker.Stop()
			return
		}
	}
}
