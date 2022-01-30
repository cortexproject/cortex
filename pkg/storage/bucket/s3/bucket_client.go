package s3

import (
	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
)

// NewBucketClient creates a new S3 bucket client
func NewBucketClient(cfg Config, name string, logger log.Logger) (objstore.Bucket, error) {
	s3Cfg, err := newS3Config(cfg)
	if err != nil {
		return nil, err
	}

	return s3.NewBucketWithConfig(logger, s3Cfg, name)
}

// NewBucketReaderClient creates a new S3 bucket client
func NewBucketReaderClient(cfg Config, name string, logger log.Logger) (objstore.BucketReader, error) {
	s3Cfg, err := newS3Config(cfg)
	if err != nil {
		return nil, err
	}

	return s3.NewBucketWithConfig(logger, s3Cfg, name)
}

func newS3Config(cfg Config) (s3.Config, error) {
	sseCfg, err := cfg.SSE.BuildThanosConfig()
	if err != nil {
		return s3.Config{}, err
	}

	return s3.Config{
		Bucket:    cfg.BucketName,
		Endpoint:  cfg.Endpoint,
		Region:    cfg.Region,
		AccessKey: cfg.AccessKeyID,
		SecretKey: cfg.SecretAccessKey.Value,
		Insecure:  cfg.Insecure,
		SSEConfig: sseCfg,
		HTTPConfig: s3.HTTPConfig{
			TransportConfig: httpconfig.TransportConfig{
				IdleConnTimeout:       int64(cfg.HTTP.IdleConnTimeout),
				ResponseHeaderTimeout: int64(cfg.HTTP.ResponseHeaderTimeout),
				TLSHandshakeTimeout:   int64(cfg.HTTP.TLSHandshakeTimeout),
				MaxIdleConns:          cfg.HTTP.MaxIdleConns,
				MaxIdleConnsPerHost:   cfg.HTTP.MaxIdleConnsPerHost,
				MaxConnsPerHost:       cfg.HTTP.MaxConnsPerHost,
			},
			TLSConfig: httpconfig.TLSConfig{
				InsecureSkipVerify: cfg.HTTP.InsecureSkipVerify,
			},
			Transport: cfg.HTTP.Transport,
		},
		// Enforce signature version 2 if CLI flag is set
		SignatureV2: cfg.SignatureVersion == SignatureVersionV2,
	}, nil
}
