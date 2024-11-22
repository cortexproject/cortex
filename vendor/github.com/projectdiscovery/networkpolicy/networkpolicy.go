package networkpolicy

import (
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"strconv"

	"github.com/gaissmai/bart"
	iputil "github.com/projectdiscovery/utils/ip"
	urlutil "github.com/projectdiscovery/utils/url"
)

func init() {
	DefaultOptions.DenyList = append(DefaultOptions.DenyList, DefaultIPv4DenylistRanges...)
	DefaultOptions.DenyList = append(DefaultOptions.DenyList, DefaultIPv6Denylist...)
	DefaultOptions.DenyList = append(DefaultOptions.DenyList, DefaultIPv6DenylistRanges...)
	DefaultOptions.DenyList = append(DefaultOptions.DenyList, DefaultIPv6Denylist...)
	DefaultOptions.AllowSchemeList = append(DefaultOptions.AllowSchemeList, DefaultSchemeAllowList...)
}

type Options struct {
	DenyList        []string
	AllowList       []string
	AllowSchemeList []string
	DenySchemeList  []string
	AllowPortList   []int
	DenyPortList    []int
}

// DefaultOptions is the base configuration for the validator
var DefaultOptions Options

type NetworkPolicy struct {
	Options    *Options
	hasFilters bool

	DenyRanger  *bart.Table[net.IP]
	AllowRanger *bart.Table[net.IP]

	AllowRules      map[string]*regexp.Regexp
	DenyRules       map[string]*regexp.Regexp
	AllowSchemeList map[string]struct{}
	DenySchemeList  map[string]struct{}
	AllowPortList   map[int]struct{}
	DenyPortList    map[int]struct{}
}

// New creates a new URL validator using the validator options
func New(options Options) (*NetworkPolicy, error) {
	allowSchemeList := make(map[string]struct{})
	for _, scheme := range options.AllowSchemeList {
		if !schemePattern.MatchString(scheme) {
			return nil, fmt.Errorf("invalid scheme format for allow list: %s", scheme)
		}
		allowSchemeList[scheme] = struct{}{}
	}

	denySchemeList := make(map[string]struct{})
	for _, scheme := range options.DenySchemeList {
		if !schemePattern.MatchString(scheme) {
			return nil, fmt.Errorf("invalid scheme format for deny list: %s", scheme)
		}
		denySchemeList[scheme] = struct{}{}
	}

	allowRules := make(map[string]*regexp.Regexp)
	denyRules := make(map[string]*regexp.Regexp)

	var allowRanger *bart.Table[net.IP]
	if len(options.AllowList) > 0 {
		allowRanger = new(bart.Table[net.IP])

		for _, r := range options.AllowList {
			// handle if ip/cidr
			cidr, err := asCidr(r)
			if err == nil {
				allowRanger.Insert(cidr, nil)
				continue
			}

			// handle as regex
			rgx, err := regexp.Compile(r)
			if err != nil {
				return nil, err
			}
			allowRules[r] = rgx
		}
	}

	var denyRanger *bart.Table[net.IP]
	if len(options.DenyList) > 0 {
		denyRanger = new(bart.Table[net.IP])

		for _, r := range options.DenyList {
			// handle if ip/cidr
			cidr, err := asCidr(r)
			if err == nil {
				denyRanger.Insert(cidr, nil)
				continue
			}

			// handle as regex
			rgx, err := regexp.Compile(r)
			if err != nil {
				return nil, err
			}
			denyRules[r] = rgx
		}
	}

	allowPortList := make(map[int]struct{})
	for _, p := range options.AllowPortList {
		allowPortList[p] = struct{}{}
	}

	denyPortList := make(map[int]struct{})
	for _, p := range options.DenyPortList {
		denyPortList[p] = struct{}{}
	}

	hasFilters := len(options.DenyList)+len(options.AllowList)+len(options.AllowSchemeList)+len(options.DenySchemeList)+len(options.AllowPortList)+len(options.DenyPortList) > 0

	return &NetworkPolicy{Options: &options, DenyRanger: denyRanger, AllowRanger: allowRanger, AllowSchemeList: allowSchemeList,
		DenySchemeList: denySchemeList, AllowRules: allowRules, DenyRules: denyRules, AllowPortList: allowPortList, DenyPortList: denyPortList, hasFilters: hasFilters}, nil
}

func (r NetworkPolicy) Validate(host string) bool {
	if !r.hasFilters {
		return true
	}

	// check if it's an ip
	if iputil.IsIP(host) {
		parsed, err := netip.ParseAddr(host)
		if err != nil {
			return false
		}

		if r.DenyRanger != nil && r.DenyRanger.Size() > 0 && rangerContains(r.DenyRanger, parsed) {
			return false
		}

		if r.AllowRanger != nil && r.AllowRanger.Size() > 0 {
			return rangerContains(r.AllowRanger, parsed)
		}

		return true
	}

	// try to obtain scheme and port
	var hasPort bool
	var port int
	var hasScheme bool
	var scheme string

	// check if it's a valid URL
	if URL, err := urlutil.Parse(host); err == nil {
		// parse scheme
		scheme := URL.Scheme
		hasScheme = scheme != ""
		// parse port
		port, err = strconv.Atoi(URL.Port())
		if err == nil {
			hasPort = true
		}
	}

	// check the port
	var isPortInDenyList, isPortInAllowedList bool

	if len(r.DenyPortList) > 0 && hasPort {
		_, isPortInDenyList = r.DenyPortList[port]
	}

	if len(r.AllowPortList) > 0 && hasPort {
		_, isPortInAllowedList = r.AllowPortList[port]
	} else {
		isPortInAllowedList = true
	}

	var isSchemeInDenyList, isSchemeInAllowedList bool
	if len(r.DenySchemeList) > 0 && hasScheme {
		_, isSchemeInDenyList = r.DenySchemeList[scheme]
	}

	if len(r.AllowSchemeList) > 0 && hasScheme {
		_, isSchemeInAllowedList = r.AllowSchemeList[scheme]
	} else {
		isSchemeInAllowedList = true
	}

	// regex
	var isInDenyList, isInAllowedList bool
	for _, r := range r.DenyRules {
		if r.MatchString(host) {
			isInDenyList = true
			break
		}
	}
	if len(r.AllowRules) > 0 {
		for _, r := range r.AllowRules {
			if r.MatchString(host) {
				isInAllowedList = true
				break
			}
		}
	} else {
		isInAllowedList = true
	}

	return !isSchemeInDenyList && !isInDenyList && isInAllowedList && isSchemeInAllowedList && !isPortInDenyList && isPortInAllowedList
}

func (r NetworkPolicy) ValidateURLWithIP(host string, ip string) bool {
	if !r.hasFilters {
		return true
	}
	return r.Validate(host) && r.ValidateAddress(ip)
}

func (r NetworkPolicy) ValidateAddress(IP string) bool {
	IPDest, err := netip.ParseAddr(IP)
	if err != nil {
		return false
	}
	if r.DenyRanger != nil && r.DenyRanger.Size() > 0 && rangerContains(r.DenyRanger, IPDest) {
		return false
	}

	if r.AllowRanger != nil && r.AllowRanger.Size() > 0 {
		return rangerContains(r.AllowRanger, IPDest)
	}

	return true
}

func (r NetworkPolicy) ValidateAddressWithPort(IP string, port int) bool {
	return r.ValidateAddress(IP) && r.ValidatePort(port)
}

func (r NetworkPolicy) ValidatePort(port int) bool {
	if r.DenyPortList != nil && portIsListed(r.DenyPortList, port) {
		return false
	}

	if r.AllowPortList != nil {
		return portIsListed(r.AllowPortList, port)
	}

	return true
}

func asCidr(s string) (netip.Prefix, error) {
	if iputil.IsIP(s) {
		s += "/32"
	}
	cidr, err := netip.ParsePrefix(s)
	if err != nil {
		return cidr, err
	}
	return cidr, nil
}

func rangerContains(ranger *bart.Table[net.IP], IP netip.Addr) bool {
	_, ok := ranger.Lookup(IP)
	return ok
}

func portIsListed(list map[int]struct{}, port int) bool {
	_, ok := list[port]
	return ok
}

// ValidateHost checks all the ips associated to a hostname and returns the valid ip if any
func (r NetworkPolicy) ValidateHost(host string) (string, bool) {
	if iputil.IsIP(host) {
		return host, r.Validate(host)
	}
	// obtain ips from system resolvers
	addrs, err := net.LookupHost(host)
	if err != nil {
		// no addresses
		return "", false
	}

	for _, addr := range addrs {
		// if at least one address
		if r.Validate(addr) {
			return addr, true
		}
	}

	return "", false
}
