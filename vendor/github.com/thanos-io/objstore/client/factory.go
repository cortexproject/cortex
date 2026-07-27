// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package client

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/azure"
	"github.com/thanos-io/objstore/providers/bos"
	"github.com/thanos-io/objstore/providers/cos"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/objstore/providers/gcs"
	"github.com/thanos-io/objstore/providers/obs"
	"github.com/thanos-io/objstore/providers/oci"
	"github.com/thanos-io/objstore/providers/oss"
	"github.com/thanos-io/objstore/providers/s3"
	"github.com/thanos-io/objstore/providers/swift"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type BucketConfig struct {
	Type   objstore.ObjProvider `yaml:"type"`
	Config interface{}          `yaml:"config"`
	Prefix string               `yaml:"prefix" default:""`
}

// NewBucket initializes and returns new object storage clients.
// NOTE: confContentYaml can contain secrets.
func NewBucket(logger log.Logger, confContentYaml []byte, component string, wrapRoundtripper func(http.RoundTripper) http.RoundTripper) (objstore.Bucket, error) {
	level.Info(logger).Log("msg", "loading bucket configuration")
	bucketConf := &BucketConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, bucketConf); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	return NewBucketFromConfig(logger, bucketConf, component, wrapRoundtripper)
}

// NewBucketFromConfig creates an objstore.Bucket from an existing BucketConfig object.
func NewBucketFromConfig(logger log.Logger, bucketConf *BucketConfig, component string, wrapRoundtripper func(http.RoundTripper) http.RoundTripper) (objstore.Bucket, error) {
	config, err := yaml.Marshal(bucketConf.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of bucket configuration")
	}

	var bucket objstore.Bucket
	switch strings.ToUpper(string(bucketConf.Type)) {
	case string(objstore.GCS):
		bucket, err = gcs.NewBucket(context.Background(), logger, config, component, wrapRoundtripper)
	case string(objstore.S3):
		bucket, err = s3.NewBucket(logger, config, component, wrapRoundtripper)
	case string(objstore.AZURE):
		bucket, err = azure.NewBucket(logger, config, component, wrapRoundtripper)
	case string(objstore.SWIFT):
		bucket, err = swift.NewContainer(logger, config, wrapRoundtripper)
	case string(objstore.COS):
		bucket, err = cos.NewBucket(logger, config, component, wrapRoundtripper)
	case string(objstore.ALIYUNOSS):
		bucket, err = oss.NewBucket(logger, config, component, wrapRoundtripper)
	case string(objstore.FILESYSTEM):
		bucket, err = filesystem.NewBucketFromConfig(config)
	case string(objstore.BOS):
		bucket, err = bos.NewBucket(logger, config, component)
	case string(objstore.OCI):
		bucket, err = oci.NewBucket(logger, config, wrapRoundtripper)
	case string(objstore.OBS):
		bucket, err = obs.NewBucket(logger, config)
	default:
		return nil, errors.Errorf("bucket with type %s is not supported", bucketConf.Type)
	}
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("create %s client", bucketConf.Type))
	}

	return objstore.NewPrefixedBucket(bucket, bucketConf.Prefix), nil
}
