package overrides

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util/services"
)

const (
	// Error messages
	ErrNoStorageBackendSpecified     = "overrides module requires a storage backend to be specified"
	ErrFilesystemBackendNotSupported = "filesystem backend is not supported for overrides module; use S3, GCS, Azure, or Swift instead"
	ErrInvalidBucketConfiguration    = "invalid bucket configuration"
	ErrInvalidOverridesConfiguration = "invalid overrides configuration"
	ErrFailedToCreateBucketClient    = "failed to create bucket client for overrides"
)

// Config holds configuration for the overrides module
type Config struct {
	// Enable the overrides API module
	// CLI flag: -overrides.enabled
	Enabled bool `yaml:"enabled"`

	// Path to the runtime configuration file that can be updated via the overrides API
	// CLI flag: -overrides.runtime-config-file
	RuntimeConfigFile string `yaml:"runtime_config_file"`

	// Storage configuration for the runtime config file
	// All bucket backends (S3, GCS, Azure, Swift) are supported, but not filesystem
	bucket.Config `yaml:",inline"`
}

// RegisterFlags registers the overrides module flags
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, "overrides.enabled", false, "Enable the overrides API module")
	f.StringVar(&c.RuntimeConfigFile, "overrides.runtime-config-file", "runtime.yaml", "Path to the runtime configuration file that can be updated via the overrides API")

	c.RegisterFlagsWithPrefix("overrides.", f)
}

// Validate validates the configuration and returns an error if validation fails
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.RuntimeConfigFile == "" {
		c.RuntimeConfigFile = "runtime.yaml"
	}

	if c.Backend == "" {
		c.Backend = bucket.S3
	}

	if c.Backend == bucket.Filesystem {
		return errors.New(ErrFilesystemBackendNotSupported)
	}

	if c.Backend == bucket.S3 {
		if c.S3.SignatureVersion == "" {
			c.S3.SignatureVersion = "v4"
		}
		if c.S3.BucketLookupType == "" {
			c.S3.BucketLookupType = "auto"
		}
		if !c.S3.SendContentMd5 {
			c.S3.SendContentMd5 = true
		}
		if c.S3.HTTP.IdleConnTimeout == 0 {
			c.S3.HTTP.IdleConnTimeout = 90 * time.Second
		}
		if c.S3.HTTP.ResponseHeaderTimeout == 0 {
			c.S3.HTTP.ResponseHeaderTimeout = 2 * time.Minute
		}
		if c.S3.HTTP.TLSHandshakeTimeout == 0 {
			c.S3.HTTP.TLSHandshakeTimeout = 10 * time.Second
		}
		if c.S3.HTTP.ExpectContinueTimeout == 0 {
			c.S3.HTTP.ExpectContinueTimeout = 1 * time.Second
		}
		if c.S3.HTTP.MaxIdleConns == 0 {
			c.S3.HTTP.MaxIdleConns = 100
		}
		if c.S3.HTTP.MaxIdleConnsPerHost == 0 {
			c.S3.HTTP.MaxIdleConnsPerHost = 100
		}
	}

	if c.Backend == bucket.Azure {
		if c.Azure.MaxRetries == 0 {
			c.Azure.MaxRetries = 20
		}
		if c.Azure.IdleConnTimeout == 0 {
			c.Azure.IdleConnTimeout = 90 * time.Second
		}
		if c.Azure.ResponseHeaderTimeout == 0 {
			c.Azure.ResponseHeaderTimeout = 2 * time.Minute
		}
		if c.Azure.TLSHandshakeTimeout == 0 {
			c.Azure.TLSHandshakeTimeout = 10 * time.Second
		}
		if c.Azure.ExpectContinueTimeout == 0 {
			c.Azure.ExpectContinueTimeout = 1 * time.Second
		}
		if c.Azure.MaxIdleConns == 0 {
			c.Azure.MaxIdleConns = 100
		}
		if c.Azure.MaxIdleConnsPerHost == 0 {
			c.Azure.MaxIdleConnsPerHost = 100
		}
	}

	if c.Backend == bucket.Swift {
		if c.Swift.AuthVersion == 0 {
			c.Swift.AuthVersion = 0
		}
		if c.Swift.MaxRetries == 0 {
			c.Swift.MaxRetries = 3
		}
		if c.Swift.ConnectTimeout == 0 {
			c.Swift.ConnectTimeout = 10 * time.Second
		}
		if c.Swift.RequestTimeout == 0 {
			c.Swift.RequestTimeout = 5 * time.Second
		}
	}

	if err := c.Config.Validate(); err != nil {
		return fmt.Errorf("%s: %w", ErrInvalidBucketConfiguration, err)
	}

	return nil
}

// API represents the overrides API module
type API struct {
	services.Service
	cfg               Config
	logger            log.Logger
	registerer        prometheus.Registerer
	bucketClient      objstore.Bucket
	runtimeConfigPath string
}

// New creates a new overrides API instance
func New(cfg Config, logger log.Logger, registerer prometheus.Registerer) (*API, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", ErrInvalidOverridesConfiguration, err)
	}

	api := &API{
		cfg:        cfg,
		logger:     logger,
		registerer: registerer,
	}
	api.Service = services.NewBasicService(api.starting, api.running, api.stopping)
	return api, nil
}

func (a *API) starting(ctx context.Context) error {
	level.Info(a.logger).Log("msg", "overrides API starting", "runtime_config_file", a.cfg.RuntimeConfigFile, "backend", a.cfg.Backend)

	bucketClient, err := bucket.NewClient(ctx, a.cfg.Config, nil, "overrides", a.logger, a.registerer)
	if err != nil {
		level.Error(a.logger).Log("msg", ErrFailedToCreateBucketClient, "err", err)
		return fmt.Errorf("%s: %w", ErrFailedToCreateBucketClient, err)
	}
	a.bucketClient = bucketClient

	a.runtimeConfigPath = a.cfg.RuntimeConfigFile

	level.Info(a.logger).Log("msg", "overrides API started successfully", "backend", a.cfg.Backend)
	return nil
}

func (a *API) running(ctx context.Context) error {
	level.Info(a.logger).Log("msg", "overrides API is now running and ready to handle requests")

	<-ctx.Done()

	level.Info(a.logger).Log("msg", "overrides API received shutdown signal")
	return nil
}

func (a *API) stopping(err error) error {
	if err != nil {
		level.Error(a.logger).Log("msg", "overrides API stopping due to error", "err", err)
	} else {
		level.Info(a.logger).Log("msg", "overrides API stopping gracefully")
	}

	level.Info(a.logger).Log("msg", "overrides API stopped")
	return nil
}
