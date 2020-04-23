package httpclient

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/prometheus/common/log"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// Config is the config for the HTTP client
type Config struct {
	Endpoint           flagext.URLValue `yaml:"endpoint"`
	ClientTimeout      time.Duration    `yaml:"client_timeout"`
	BackendReadTimeout time.Duration    `yaml:"backend_read_timeout"`

	TLSCertPath string `yaml:"tls_cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path"`
	TLSCAPath   string `yaml:"tls_ca_path"`
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func ConfigFromURLAndTimeout(u *url.URL, timeout time.Duration) *Config {
	return &Config{
		Endpoint:      flagext.URLValue{URL: u},
		ClientTimeout: timeout,
	}
}

// RegisterFlagsWithPrefix registers flags with prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&cfg.Endpoint, prefix+".http-endpoint", "Endpoint to connect to")
	f.DurationVar(&cfg.ClientTimeout, prefix+".http-client-timeout", 5*time.Second, "Timeout for connecting to the endpoint")
	f.DurationVar(&cfg.BackendReadTimeout, prefix+".http-backend-read-timeout", 90*time.Second, "Timeout for reading from backend")

	f.StringVar(&cfg.TLSCertPath, prefix+".http-tls-cert-path", "", "TLS cert path for the HTTP client")
	f.StringVar(&cfg.TLSKeyPath, prefix+".http-tls-key-path", "", "TLS key path for the HTTP client")
	f.StringVar(&cfg.TLSCAPath, prefix+".http-tls-ca-path", "", "TLS CA path for the HTTP client")
}

// GetTLSConfig initialises tls.Config from config options.
func (cfg *Config) GetTLSConfig() *tls.Config {
	if cfg.TLSCertPath != "" && cfg.TLSKeyPath != "" && cfg.TLSCAPath != "" {
		clientCert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
		if err != nil {
			log.Warnf("error loading cert %s or key %s, tls disabled", cfg.TLSCertPath, cfg.TLSKeyPath)
		}

		var caCertPool *x509.CertPool
		caCert, err := ioutil.ReadFile(cfg.TLSCAPath)
		if err != nil {
			log.Warnf("error loading ca cert %s, tls disabled", cfg.TLSCAPath)
		} else {
			caCertPool = x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
		}
		if len(clientCert.Certificate) > 0 && caCertPool != nil {
			return &tls.Config{
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{clientCert},
				RootCAs:            caCertPool,
			}
		}
	}
	return nil
}
