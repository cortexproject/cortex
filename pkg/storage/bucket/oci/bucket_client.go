package oci

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/oci"
	yaml "gopkg.in/yaml.v2"
)

// NewBucketClient creates a new OCI bucket client.
func NewBucketClient(cfg Config, hedgedRoundTripper func(rt http.RoundTripper) http.RoundTripper, _ string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := oci.DefaultConfig
	bucketConfig.Provider = cfg.Provider
	bucketConfig.Bucket = cfg.Bucket
	bucketConfig.Compartment = cfg.Compartment
	bucketConfig.Tenancy = cfg.Tenancy
	bucketConfig.User = cfg.User
	bucketConfig.Region = cfg.Region
	bucketConfig.Fingerprint = cfg.Fingerprint
	bucketConfig.PrivateKey = cfg.PrivateKey.Value
	bucketConfig.Passphrase = cfg.Passphrase.Value
	bucketConfig.PartSize = cfg.PartSize
	bucketConfig.MaxRequestRetries = cfg.MaxRequestRetries
	bucketConfig.RequestRetryInterval = cfg.RequestRetryInterval

	serialized, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, err
	}

	return oci.NewBucket(logger, serialized, hedgedRoundTripper)
}
