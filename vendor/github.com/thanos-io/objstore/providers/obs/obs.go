// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package obs

import (
	"context"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/exthttp"

	"github.com/go-kit/log"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

const DirDelim = "/"

const (
	MinMultipartUploadSize int64 = 1024 * 1024 * 100
	PartSize               int64 = 1024 * 1024 * 100
)

var DefaultConfig = Config{
	HTTPConfig: exthttp.HTTPConfig{
		IdleConnTimeout:       model.Duration(90 * time.Second),
		ResponseHeaderTimeout: model.Duration(2 * time.Minute),
		TLSHandshakeTimeout:   model.Duration(10 * time.Second),
		ExpectContinueTimeout: model.Duration(1 * time.Second),
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       0,
	},
}

type Config struct {
	Bucket     string             `yaml:"bucket"`
	Endpoint   string             `yaml:"endpoint"`
	AccessKey  string             `yaml:"access_key"`
	SecretKey  string             `yaml:"secret_key"`
	MaxRetries int                `yaml:"max_retries"`
	HTTPConfig exthttp.HTTPConfig `yaml:"http_config"`
}

func (conf *Config) validate() error {
	if conf.Endpoint == "" {
		return errors.New("no obs endpoint in config file")
	}

	if conf.AccessKey == "" && conf.SecretKey != "" {
		return errors.New("no obs access_key specified")
	}

	if conf.AccessKey != "" && conf.SecretKey == "" {
		return errors.New("no obs secret_key specified")
	}

	if conf.AccessKey == "" && conf.SecretKey == "" {
		return errors.New("no obs secret_key and access_key specified")
	}
	return nil
}

type Bucket struct {
	logger log.Logger
	client *obs.ObsClient
	name   string
}

func NewBucket(logger log.Logger, conf []byte) (*Bucket, error) {
	// TODO(https://github.com/thanos-io/objstore/pull/150): Add support for roundtripper wrapper.
	config, err := parseConfig(conf)
	if err != nil {
		return nil, errors.Wrap(err, "parsing cos configuration")
	}
	return NewBucketWithConfig(logger, config)
}

func parseConfig(conf []byte) (Config, error) {
	config := DefaultConfig
	if err := yaml.UnmarshalStrict(conf, &config); err != nil {
		return Config{}, err
	}

	return config, nil
}

func NewBucketWithConfig(logger log.Logger, config Config) (*Bucket, error) {
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "validate obs config err")
	}

	rt, err := exthttp.DefaultTransport(config.HTTPConfig)
	if err != nil {
		return nil, errors.Wrap(err, "get http transport err")
	}

	var client *obs.ObsClient
	if config.MaxRetries > 0 {
		client, err = obs.New(config.AccessKey, config.SecretKey, config.Endpoint, obs.WithHttpTransport(rt), obs.WithMaxRetryCount(config.MaxRetries))
	} else {
		client, err = obs.New(config.AccessKey, config.SecretKey, config.Endpoint, obs.WithHttpTransport(rt))
	}

	if err != nil {
		return nil, errors.Wrap(err, "initialize obs client err")
	}

	bkt := &Bucket{
		logger: logger,
		client: client,
		name:   config.Bucket,
	}
	return bkt, nil
}

func (b *Bucket) Provider() objstore.ObjProvider { return objstore.OBS }

// Name returns the bucket name for the provider.
func (b *Bucket) Name() string {
	return b.name
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	input := &obs.DeleteObjectInput{Bucket: b.name, Key: name}
	_, err := b.client.DeleteObject(input)
	return err
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	size, err := objstore.TryToGetSize(r)

	if err != nil {
		return errors.Wrapf(err, "failed to get size apriori to upload %s", name)
	}

	if size <= 0 {
		return errors.New("object size must be provided")
	}

	uploadOpts := objstore.ApplyObjectUploadOptions(opts...)
	if size <= MinMultipartUploadSize {
		err = b.putObjectSingle(name, r, uploadOpts)
		if err != nil {
			return err
		}
	} else {
		var initOutput *obs.InitiateMultipartUploadOutput
		initOutput, err = b.initiateMultipartUpload(name, uploadOpts)
		if err != nil {
			return err
		}

		uploadId := initOutput.UploadId
		defer func() {
			if err != nil {
				if _, err = b.client.AbortMultipartUpload(&obs.AbortMultipartUploadInput{
					UploadId: uploadId,
					Bucket:   b.name,
					Key:      name,
				}); err != nil {
					err = errors.Wrap(err, "failed to abort multipart upload")
					return
				}
			}
		}()
		parts, err := b.multipartUpload(size, name, uploadId, r)
		if err != nil {
			return err
		}

		_, err = b.client.CompleteMultipartUpload(&obs.CompleteMultipartUploadInput{
			Bucket:   b.name,
			Key:      name,
			UploadId: uploadId,
			Parts:    parts,
		})
		if err != nil {
			return errors.Wrap(err, "failed to complete multipart upload")
		}
	}
	return nil
}

func (b *Bucket) putObjectSingle(key string, body io.Reader, opts objstore.UploadObjectParams) error {
	input := &obs.PutObjectInput{}
	input.Bucket = b.name
	input.Key = key
	input.Body = body
	input.ContentType = opts.ContentType
	_, err := b.client.PutObject(input)
	if err != nil {
		return errors.Wrap(err, "failed to upload object")
	}
	return nil
}

func (b *Bucket) initiateMultipartUpload(key string, opts objstore.UploadObjectParams) (output *obs.InitiateMultipartUploadOutput, err error) {
	initInput := &obs.InitiateMultipartUploadInput{}
	initInput.Bucket = b.name
	initInput.Key = key
	initInput.ContentType = opts.ContentType
	initOutput, err := b.client.InitiateMultipartUpload(initInput)
	if err != nil {
		return nil, errors.Wrap(err, "failed to init multipart upload job")
	}
	return initOutput, nil
}

func (b *Bucket) multipartUpload(size int64, key, uploadId string, body io.Reader) ([]obs.Part, error) {
	partSum := int(math.Ceil(float64(size) / float64(PartSize)))
	lastPart := size % PartSize
	parts := make([]obs.Part, 0, partSum)
	for i := 1; i <= partSum; i++ {
		partSize := PartSize
		if i == partSum {
			partSize = lastPart
		}
		output, err := b.client.UploadPart(&obs.UploadPartInput{
			Bucket:     b.name,
			Key:        key,
			UploadId:   uploadId,
			Body:       body,
			PartNumber: i,
			PartSize:   partSize,
			Offset:     int64(i-1) * PartSize,
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to multipart upload")
		}
		parts = append(parts, obs.Part{PartNumber: output.PartNumber, ETag: output.ETag})
	}
	return parts, nil
}

func (b *Bucket) Close() error { return nil }

func (b *Bucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{objstore.Recursive}
}

// Iter calls f for each entry in the given directory (not recursive.)
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}

	input := &obs.ListObjectsInput{}
	input.Bucket = b.name
	input.Prefix = dir
	input.Delimiter = DirDelim
	if objstore.ApplyIterOptions(options...).Recursive {
		input.Delimiter = ""
	}
	for {
		output, err := b.client.ListObjects(input)
		if err != nil {
			return errors.Wrap(err, "failed to list object")
		}
		for _, content := range output.Contents {
			if err := f(content.Key); err != nil {
				return errors.Wrapf(err, "failed to call iter function for object %s", content.Key)
			}
		}
		for _, topDir := range output.CommonPrefixes {
			if err := f(topDir); err != nil {
				return errors.Wrapf(err, "failed to call iter function for top dir object %s", topDir)
			}
		}

		if !output.IsTruncated {
			break
		}

		input.Marker = output.NextMarker
	}
	return nil
}

func (b *Bucket) IterWithAttributes(ctx context.Context, dir string, f func(attrs objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if err := objstore.ValidateIterOptions(b.SupportedIterOptions(), options...); err != nil {
		return err
	}

	return b.Iter(ctx, dir, func(name string) error {
		return f(objstore.IterObjectAttributes{Name: name})
	}, options...)
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

func (b *Bucket) getRange(_ context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("object name cannot be empty")
	}
	input := &obs.GetObjectInput{}
	input.Bucket = b.name
	input.Key = name
	if off < 0 {
		return nil, errors.New("incorrect offset")
	}
	input.RangeStart = off
	input.RangeEnd = math.MaxInt64
	if length != -1 {
		input.RangeEnd = off + length - 1
	}
	output, err := b.client.GetObject(input)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get object")
	}
	return objstore.ObjectSizerReadCloser{
		ReadCloser: output.Body,
		Size: func() (int64, error) {
			return output.ContentLength, nil
		},
	}, nil
}

// Exists checks if the given object exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	_, err := b.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: b.name,
		Key:    name,
	})
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "failed to get object metadata")
	}
	return true, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	if oriErr, ok := errors.Cause(err).(obs.ObsError); ok {
		if oriErr.Status == "404 Not Found" {
			return true
		}
	}
	return false
}

// IsAccessDeniedErr returns true if access to object is denied.
func (b *Bucket) IsAccessDeniedErr(_ error) bool {
	return false
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	output, err := b.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: b.name,
		Key:    name,
	})
	if err != nil {
		return objstore.ObjectAttributes{}, errors.Wrap(err, "failed to get object metadata")
	}
	return objstore.ObjectAttributes{
		Size:         output.ContentLength,
		LastModified: output.LastModified,
	}, nil
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
func NewTestBucket(t testing.TB, location string) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if c.Endpoint == "" || c.AccessKey == "" || c.SecretKey == "" {
		return nil, nil, errors.New("insufficient obs test configuration information")
	}

	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
		return nil, nil, errors.New("OBS_BUCKET is defined. Normally this tests will create temporary bucket " +
			"and delete it after test. Unset OBS_BUCKET env variable to use default logic. If you really want to run " +
			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true.")
	}
	return NewTestBucketFromConfig(t, c, false, location)
}

func NewTestBucketFromConfig(t testing.TB, c Config, reuseBucket bool, location string) (objstore.Bucket, func(), error) {
	ctx := context.Background()

	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}
	b, err := NewBucket(log.NewNopLogger(), bc)
	if err != nil {
		return nil, nil, err
	}

	bktToCreate := c.Bucket
	if c.Bucket != "" && reuseBucket {
		if err := b.Iter(ctx, "", func(_ string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, err
		}

		t.Log("WARNING. Reusing", c.Bucket, "OBS bucket for OBS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	if c.Bucket == "" {
		bktToCreate = objstore.CreateTemporaryTestBucketName(t)
	}

	_, err = b.client.CreateBucket(&obs.CreateBucketInput{
		Bucket:         bktToCreate,
		BucketLocation: obs.BucketLocation{Location: location},
	})
	if err != nil {
		return nil, nil, err
	}
	b.name = bktToCreate
	t.Log("created temporary OBS bucket for OBS tests with name", bktToCreate)

	return b, func() {
		objstore.EmptyBucket(t, ctx, b)
		if _, err := b.client.DeleteBucket(bktToCreate); err != nil {
			t.Logf("deleting bucket %s failed: %s", bktToCreate, err)
		}
	}, nil
}

func configFromEnv() Config {
	c := Config{
		Bucket:    os.Getenv("OBS_BUCKET"),
		Endpoint:  os.Getenv("OBS_ENDPOINT"),
		AccessKey: os.Getenv("OBS_ACCESS_KEY"),
		SecretKey: os.Getenv("OBS_SECRET_KEY"),
	}
	return c
}
