package bucket

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"

	cortex_s3 "github.com/cortexproject/cortex/pkg/storage/bucket/s3"
)

// TenantConfigProvider defines a per-tenant config provider.
type TenantConfigProvider interface {
	// S3SSEType returns the per-tenant S3 SSE type.
	S3SSEType(user string) string

	// S3SSEKMSKeyID returns the per-tenant S3 KMS-SSE key id or an empty string if not set.
	S3SSEKMSKeyID(userID string) string

	// S3SSEKMSEncryptionContext returns the per-tenant S3 KMS-SSE key id or an empty string if not set.
	S3SSEKMSEncryptionContext(userID string) string
}

// UserBucketReaderClient is a wrapper around a objstore.BucketReader that reads from user-specific subfolder.
type UserBucketReaderClient struct {
	userID string
	bucket objstore.BucketReader
}

// UserBucketClient is a wrapper around a objstore.Bucket that prepends writes with a userID
type UserBucketClient struct {
	UserBucketReaderClient
	bucket      objstore.Bucket
	cfgProvider TenantConfigProvider
}

// NewUserBucketClient makes a new UserBucketClient. The cfgProvider can be nil.
func NewUserBucketClient(userID string, bucket objstore.Bucket, cfgProvider TenantConfigProvider) *UserBucketClient {
	return &UserBucketClient{
		UserBucketReaderClient: UserBucketReaderClient{
			userID: userID,
			bucket: bucket,
		},
		bucket:      bucket,
		cfgProvider: cfgProvider,
	}
}

func (b *UserBucketReaderClient) fullName(name string) string {
	return fmt.Sprintf("%s/%s", b.userID, name)
}

// Close implements io.Closer
func (b *UserBucketClient) Close() error { return b.bucket.Close() }

// Upload the contents of the reader as an object into the bucket.
func (b *UserBucketClient) Upload(ctx context.Context, name string, r io.Reader) error {
	if sse, err := b.getCustomS3SSEConfig(); err != nil {
		return err
	} else if sse != nil {
		// If the underlying bucket client is not S3 and a custom S3 SSE config has been
		// provided, the config option will be ignored.
		ctx = s3.ContextWithSSEConfig(ctx, sse)
	}

	return b.bucket.Upload(ctx, b.fullName(name), r)
}

// Delete removes the object with the given name.
func (b *UserBucketClient) Delete(ctx context.Context, name string) error {
	return b.bucket.Delete(ctx, b.fullName(name))
}

// Name returns the bucket name for the provider.
func (b *UserBucketClient) Name() string { return b.bucket.Name() }

func (b *UserBucketClient) getCustomS3SSEConfig() (encrypt.ServerSide, error) {
	if b.cfgProvider == nil {
		return nil, nil
	}

	// No S3 SSE override if the type override hasn't been provided.
	sseType := b.cfgProvider.S3SSEType(b.userID)
	if sseType == "" {
		return nil, nil
	}

	cfg := cortex_s3.SSEConfig{
		Type:                 sseType,
		KMSKeyID:             b.cfgProvider.S3SSEKMSKeyID(b.userID),
		KMSEncryptionContext: b.cfgProvider.S3SSEKMSEncryptionContext(b.userID),
	}

	sse, err := cfg.BuildMinioConfig()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to customise S3 SSE config for tenant %s", b.userID)
	}

	return sse, nil
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *UserBucketReaderClient) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return b.bucket.Iter(ctx, b.fullName(dir), func(s string) error {
		/*
			Since all objects are prefixed with the userID we need to strip the userID
			upon passing to the processing function
		*/
		return f(strings.Join(strings.Split(s, "/")[1:], "/"))
	}, options...)
}

// Get returns a reader for the given object name.
func (b *UserBucketReaderClient) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.bucket.Get(ctx, b.fullName(name))
}

// GetRange returns a new range reader for the given object name and range.
func (b *UserBucketReaderClient) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.bucket.GetRange(ctx, b.fullName(name), off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *UserBucketReaderClient) Exists(ctx context.Context, name string) (bool, error) {
	return b.bucket.Exists(ctx, b.fullName(name))
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *UserBucketReaderClient) IsObjNotFoundErr(err error) bool {
	return b.bucket.IsObjNotFoundErr(err)
}

// Attributes returns attributes of the specified object.
func (b *UserBucketReaderClient) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return b.bucket.Attributes(ctx, b.fullName(name))
}

// ReaderWithExpectedErrs allows to specify a filter that marks certain errors as expected, so it will not increment
// thanos_objstore_bucket_operation_failures_total metric.
func (b *UserBucketReaderClient) ReaderWithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	if ib, ok := b.bucket.(objstore.InstrumentedBucketReader); ok {
		return &UserBucketReaderClient{
			userID: b.userID,
			bucket: ib.ReaderWithExpectedErrs(fn),
		}
	}

	return b
}

// WithExpectedErrs allows to specify a filter that marks certain errors as expected, so it will not increment
// thanos_objstore_bucket_operation_failures_total metric.
func (b *UserBucketClient) WithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ib, ok := b.bucket.(objstore.InstrumentedBucket); ok {
		nb := ib.WithExpectedErrs(fn)

		return &UserBucketClient{
			UserBucketReaderClient: UserBucketReaderClient{
				userID: b.userID,
				bucket: nb,
			},
			bucket: nb,
		}
	}

	return b
}
