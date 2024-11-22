package fastdialer

import (
	"log"
	"net"
	"time"

	"github.com/projectdiscovery/networkpolicy"
	"golang.org/x/net/proxy"
)

// DefaultResolvers trusted
var DefaultResolvers = []string{
	"1.1.1.1:53",
	"1.0.0.1:53",
	"8.8.8.8:53",
	"8.8.4.4:53",
}

type CacheType uint8

const (
	Memory CacheType = iota
	Disk
	Hybrid
)

type DiskDBType uint8

const (
	LevelDB DiskDBType = iota
	Pogreb
)

type Options struct {
	BaseResolvers            []string
	MaxRetries               int
	HostsFile                bool
	ResolversFile            bool
	EnableFallback           bool
	Allow                    []string
	Deny                     []string
	AllowSchemeList          []string
	DenySchemeList           []string
	AllowPortList            []int
	DenyPortList             []int
	CacheType                CacheType
	CacheMemoryMaxItems      int // used by Memory cache type
	DiskDbType               DiskDBType
	WithDialerHistory        bool
	WithCleanup              bool
	WithTLSData              bool
	DialerTimeout            time.Duration
	DialerKeepAlive          time.Duration
	Dialer                   *net.Dialer
	ProxyDialer              *proxy.Dialer
	WithZTLS                 bool
	SNIName                  string
	OnBeforeDial             func(hostname, IP, port string)
	OnInvalidTarget          func(hostname, IP, port string)
	OnDialCallback           func(hostname, IP string)
	DisableZtlsFallback      bool
	WithNetworkPolicyOptions *networkpolicy.Options
	// optional network policy override for sharing
	NetworkPolicy *networkpolicy.NetworkPolicy
	// optional logger to log errors(like hostfile init error)
	Logger *log.Logger
	// optional max temporary errors to mark as permanent
	MaxTemporaryErrors              int
	MaxTemporaryToPermanentDuration time.Duration
}

// DefaultOptions of the cache
var DefaultOptions = Options{
	BaseResolvers:                   DefaultResolvers,
	MaxRetries:                      5,
	HostsFile:                       true,
	ResolversFile:                   true,
	CacheType:                       Disk,
	DialerTimeout:                   10 * time.Second,
	DialerKeepAlive:                 10 * time.Second,
	MaxTemporaryErrors:              30,
	MaxTemporaryToPermanentDuration: time.Minute,
}
