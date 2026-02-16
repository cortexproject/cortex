package overrides

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util/runtimeconfig"
	"github.com/cortexproject/cortex/pkg/util/services"
)

const (
	ErrInvalidOverridesConfiguration = "invalid overrides configuration"
	ErrFailedToCreateBucketClient    = "failed to create bucket client for overrides"
)

type API struct {
	services.Service
	cfg               runtimeconfig.Config
	logger            log.Logger
	registerer        prometheus.Registerer
	bucketClient      objstore.Bucket
	runtimeConfigPath string
}

func New(cfg runtimeconfig.Config, logger log.Logger, registerer prometheus.Registerer) (*API, error) {
	if err := cfg.StorageConfig.Validate(); err != nil {
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
	level.Info(a.logger).Log(
		"msg", "overrides API starting",
		"runtime_config_file", a.cfg.LoadPath,
		"backend", a.cfg.StorageConfig.Backend,
		"s3_endpoint", a.cfg.StorageConfig.S3.Endpoint,
		"response_header_timeout", a.cfg.StorageConfig.S3.HTTP.ResponseHeaderTimeout,
		"idle_conn_timeout", a.cfg.StorageConfig.S3.HTTP.IdleConnTimeout,
		"tls_handshake_timeout", a.cfg.StorageConfig.S3.HTTP.TLSHandshakeTimeout,
	)

	bucketClient, err := bucket.NewClient(ctx, a.cfg.StorageConfig, nil, "overrides", a.logger, a.registerer)
	if err != nil {
		level.Error(a.logger).Log("msg", ErrFailedToCreateBucketClient, "err", err)
		return fmt.Errorf("%s: %w", ErrFailedToCreateBucketClient, err)
	}
	a.bucketClient = bucketClient

	a.runtimeConfigPath = a.cfg.LoadPath

	level.Info(a.logger).Log("msg", "overrides API started successfully", "backend", a.cfg.StorageConfig.Backend)
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

	// Close bucket client to release resources
	if a.bucketClient != nil {
		if closeErr := a.bucketClient.Close(); closeErr != nil {
			level.Warn(a.logger).Log("msg", "failed to close bucket client", "err", closeErr)
		}
	}

	level.Info(a.logger).Log("msg", "overrides API stopped")
	return nil
}
