package grpcclient

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/cortexproject/cortex/pkg/util"
)

// Config for a gRPC client.
type Config struct {
	MaxRecvMsgSize     int     `yaml:"max_recv_msg_size"`
	MaxSendMsgSize     int     `yaml:"max_send_msg_size"`
	UseGzipCompression bool    `yaml:"use_gzip_compression"`
	RateLimit          float64 `yaml:"rate_limit"`
	RateLimitBurst     int     `yaml:"rate_limit_burst"`

	BackoffOnRatelimits bool               `yaml:"backoff_on_ratelimits"`
	BackoffConfig       util.BackoffConfig `yaml:"backoff_config"`

	TLSCertPath string `yaml:"tls_cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path"`
	TLSCAPath   string `yaml:"tls_ca_path"`
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlags(prefix string, f *flag.FlagSet) {
	f.IntVar(&cfg.MaxRecvMsgSize, prefix+".grpc-max-recv-msg-size", 100<<20, "gRPC client max receive message size (bytes).")
	f.IntVar(&cfg.MaxSendMsgSize, prefix+".grpc-max-send-msg-size", 16<<20, "gRPC client max send message size (bytes).")
	f.BoolVar(&cfg.UseGzipCompression, prefix+".grpc-use-gzip-compression", false, "Use compression when sending messages.")
	f.Float64Var(&cfg.RateLimit, prefix+".grpc-client-rate-limit", 0., "Rate limit for gRPC client; 0 means disabled.")
	f.IntVar(&cfg.RateLimitBurst, prefix+".grpc-client-rate-limit-burst", 0, "Rate limit burst for gRPC client.")
	f.BoolVar(&cfg.BackoffOnRatelimits, prefix+".backoff-on-ratelimits", false, "Enable backoff and retry when we hit ratelimits.")

	cfg.BackoffConfig.RegisterFlags(prefix, f)

	f.StringVar(&cfg.TLSCertPath, prefix+".tls-cert-path", "", "gRPC TLS cert path.")
	f.StringVar(&cfg.TLSKeyPath, prefix+".tls-key-path", "", "gRPC TLS key path.")
	f.StringVar(&cfg.TLSCAPath, prefix+".tls-ca-path", "", "gRPC TLS CA path.")
}

// CallOptions returns the config in terms of CallOptions.
func (cfg *Config) CallOptions() []grpc.CallOption {
	var opts []grpc.CallOption
	opts = append(opts, grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize))
	opts = append(opts, grpc.MaxCallSendMsgSize(cfg.MaxSendMsgSize))
	if cfg.UseGzipCompression {
		opts = append(opts, grpc.UseCompressor("gzip"))
	}
	return opts
}

// DialOption returns the config as a grpc.DialOptions.
func (cfg *Config) DialOption(unaryClientInterceptors []grpc.UnaryClientInterceptor, streamClientInterceptors []grpc.StreamClientInterceptor) []grpc.DialOption {
	if cfg.BackoffOnRatelimits {
		unaryClientInterceptors = append([]grpc.UnaryClientInterceptor{NewBackoffRetry(cfg.BackoffConfig)}, unaryClientInterceptors...)
	}

	if cfg.RateLimit > 0 {
		unaryClientInterceptors = append([]grpc.UnaryClientInterceptor{NewRateLimiter(cfg)}, unaryClientInterceptors...)
	}

	var opts []grpc.DialOption
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
			tlsConfig := &tls.Config{
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{clientCert},
				RootCAs:            caCertPool,
			}
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		}
	}

	return append(opts, grpc.WithDefaultCallOptions(cfg.CallOptions()...),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(unaryClientInterceptors...)),
		grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(streamClientInterceptors...)),
	)
}
