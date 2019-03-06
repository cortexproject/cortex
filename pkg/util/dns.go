package util

import (
	"fmt"
	"net"

	"github.com/go-kit/kit/log/level"
	"github.com/miekg/dns"
)

const defaultResolvConf = "/etc/resolv.conf"

func getDefaultDNSServers(conf *dns.ClientConfig) []string {
	servers := make([]string, 0, len(conf.Servers))
	for _, server := range conf.Servers {
		servers = append(servers, net.JoinHostPort(server, conf.Port))
	}

	return servers
}

// LookupSRV tries to resolve an SRV query of the given service, proto and domain name.
// proto can be 'tcp' or udp'.
// The query will be of the form _service._proto.name.
func LookupSRV(service, proto, name string) ([]*dns.SRV, error) {
	conf, err := dns.ClientConfigFromFile(defaultResolvConf)
	if err != nil {
		return nil, err
	}

	dnsServers := getDefaultDNSServers(conf)

	name = "_" + service + "._" + proto + "." + name
	names := conf.NameList(name)

	dnsResolved := false
	client := dns.Client{}

	for _, name := range names {
		msg := &dns.Msg{}
		msg.SetQuestion(dns.Fqdn(name), dns.TypeSRV)

		var result []*dns.SRV
		for _, serverAddr := range dnsServers {
			resMsg, _, err := client.Exchange(msg, serverAddr)
			if err != nil {
				level.Warn(Logger).Log("msg", "DNS exchange failed", "err", err)
				continue
			}
			dnsResolved = true
			for _, ans := range resMsg.Answer {
				if srvRecord, ok := ans.(*dns.SRV); ok {
					result = append(result, srvRecord)
				}
			}
			if len(result) > 0 {
				return result, nil
			}
		}
	}

	if !dnsResolved {
		return nil, fmt.Errorf("Couldn't resolve %s: No server responded", name)
	}

	return nil, nil
}
