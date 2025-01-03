package azure

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/exthttp"
	"github.com/thanos-io/objstore/providers/azure"
)

func NewBucketClient(cfg Config, hedgedRoundTripper func(rt http.RoundTripper) http.RoundTripper, name string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := azure.Config{
		StorageAccountName:      cfg.StorageAccountName,
		StorageAccountKey:       cfg.StorageAccountKey.Value,
		StorageConnectionString: cfg.StorageConnectionString.Value,
		ContainerName:           cfg.ContainerName,
		Endpoint:                cfg.Endpoint,
		MaxRetries:              cfg.MaxRetries,
		UserAssignedID:          cfg.UserAssignedID,
		HTTPConfig: exthttp.HTTPConfig{
			IdleConnTimeout:       model.Duration(cfg.IdleConnTimeout),
			ResponseHeaderTimeout: model.Duration(cfg.ResponseHeaderTimeout),
			InsecureSkipVerify:    cfg.InsecureSkipVerify,
			TLSHandshakeTimeout:   model.Duration(cfg.TLSHandshakeTimeout),
			ExpectContinueTimeout: model.Duration(cfg.ExpectContinueTimeout),
			MaxIdleConns:          cfg.MaxIdleConns,
			MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
			MaxConnsPerHost:       cfg.MaxConnsPerHost,
		},
	}

	return azure.NewBucketWithConfig(logger, bucketConfig, name, hedgedRoundTripper)
}
