package httpclient

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/prometheus/common/log"
)

// Config is the config for the HTTP client
type Config struct {
	HTTPEndpoint      flagext.URLValue `yaml:"http_endpoint"`
	HTTPClientTimeout time.Duration    `yaml:"http_client_timeout"`

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
		HTTPEndpoint:      flagext.URLValue{URL: u},
		HTTPClientTimeout: timeout,
	}
}

// RegisterFlagsWithPrefix registers flags with prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&cfg.HTTPEndpoint, prefix+".http-endpoint", "Endpoint to connect to")
	f.DurationVar(&cfg.HTTPClientTimeout, prefix+".http-client-timeout", 5*time.Second, "Timeout for connecting to the endpoint.")

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
