package tls

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ClientConfig is the config for client TLS.
type ClientConfig struct {
	CertPath string `yaml:"tls_cert_path"`
	KeyPath  string `yaml:"tls_key_path"`
	CAPath   string `yaml:"tls_ca_path"`
}

// RegisterFlagsWithPrefix registers flags with prefix.
func (cfg *ClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.CertPath, prefix+".tls-cert-path", "", "TLS cert path for the client")
	f.StringVar(&cfg.KeyPath, prefix+".tls-key-path", "", "TLS key path for the client")
	f.StringVar(&cfg.CAPath, prefix+".tls-ca-path", "", "TLS CA path for the client")
}

// GetTLSConfig initialises tls.Config from config options
func (cfg *ClientConfig) GetTLSConfig() (*tls.Config, error) {
	if cfg.CertPath != "" && cfg.KeyPath != "" && cfg.CAPath != "" {
		clientCert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certs: %v", err)
		}

		var caCertPool *x509.CertPool
		caCert, err := ioutil.ReadFile(cfg.CAPath)
		if err != nil {
			return nil, fmt.Errorf("error loading ca cert: %v", err)
		}
		caCertPool = x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		if len(clientCert.Certificate) > 0 && caCertPool != nil {
			return &tls.Config{
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{clientCert},
				RootCAs:            caCertPool,
			}, nil
		}
	}
	return nil, nil
}

// GetGRPCDialOptions creates GRPC DialOptions for TLS
func (cfg *ClientConfig) GetGRPCDialOptions() ([]grpc.DialOption, error) {
	if tlsConfig, err := cfg.GetTLSConfig(); err != nil {
		return nil, fmt.Errorf("error creating grpc dial options: %v", err)
	} else if tlsConfig != nil {
		return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))}, nil
	} else {
		return []grpc.DialOption{grpc.WithInsecure()}, nil
	}
	return nil, nil
}
