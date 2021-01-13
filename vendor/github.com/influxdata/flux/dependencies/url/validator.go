package url

import (
	"fmt"
	"net"
	"net/url"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
)

// Validator reports whether a given URL is valid.
type Validator interface {
	Validate(*url.URL) error
}

// PassValidator passes all URLs
type PassValidator struct{}

func (PassValidator) Validate(*url.URL) error {
	return nil
}

// PrivateIPValidator validates that a url does not communicate with a private IP range
type PrivateIPValidator struct{}

func (PrivateIPValidator) Validate(u *url.URL) error {
	ips, err := net.LookupIP(u.Hostname())
	if err != nil {
		return err
	}
	for _, ip := range ips {
		if isPrivateIP(ip) {
			return errors.New(codes.Invalid, "url is not valid, it connects to a private IP")
		}
	}
	return nil
}

// privateIPBlocks is a list of IP ranges that are defined as private.
var privateIPBlocks []*net.IPNet

func init() {
	for _, cidr := range []string{
		"127.0.0.0/8",    // IPv4 loopback
		"10.0.0.0/8",     // RFC1918
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
		"169.254.0.0/16", // RFC3927
		"::1/128",        // IPv6 loopback
		"fe80::/10",      // IPv6 link-local
		"fc00::/7",       // IPv6 unique local addr
	} {
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(fmt.Errorf("parse error on %q: %v", cidr, err))
		}
		privateIPBlocks = append(privateIPBlocks, block)
	}
}

//  isPrivateIP reports whether an IP exists in a known private IP space.
func isPrivateIP(ip net.IP) bool {
	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}
