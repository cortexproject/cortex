package util

import (
	"fmt"

	"github.com/miekg/dns"
)

const defaultResolvConf = "/etc/resolv.conf"

// GetDefaultDNSServers returns the DNS servers from the default
// resolver configuration.
func GetDefaultDNSServers() ([]string, error) {
	conf, err := dns.ClientConfigFromFile(defaultResolvConf)
	if err != nil {
		return nil, err
	}
	return conf.Servers, nil
}

// LookupSRV tries to resolve an SRV query of the given service, proto and domain name.
// proto can be 'tcp' or udp'.
// The query will be of the form _service._proto.name.
func LookupSRV(service, proto, name string) ([]*dns.SRV, error) {
	dnsServers, err := GetDefaultDNSServers()
	if err != nil {
		return nil, err
	}

	msg := &dns.Msg{}
	name = "_" + service + "._" + proto + "." + name
	msg.SetQuestion(dns.Fqdn(name), dns.TypeSRV)

	var result []*dns.SRV
	dnsResolved := false
	for _, serverAddr := range dnsServers {
		resMsg, err := dns.Exchange(msg, serverAddr)
		if err != nil {
			continue
		}
		dnsResolved = true
		for _, ans := range resMsg.Ns {
			if srvRecord, ok := ans.(*dns.SRV); ok {
				result = append(result, srvRecord)
			}
		}
		if len(result) > 0 {
			return result, nil
		}
	}

	if !dnsResolved {
		return result, fmt.Errorf("Couldn't resolve %s: No server responded", name)
	}

	return result, nil
}
