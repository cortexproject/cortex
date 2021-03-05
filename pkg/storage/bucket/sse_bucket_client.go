package bucket

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"

	cortex_s3 "github.com/cortexproject/cortex/pkg/storage/bucket/s3"
)

// TenantConfigProvider defines a per-tenant config provider.
type TenantConfigProvider interface {
	// S3SSEType returns the per-tenant S3 SSE type.
	S3SSEType(userID string) string

	// S3SSEKMSKeyID returns the per-tenant S3 KMS-SSE key id or an empty string if not set.
	S3SSEKMSKeyID(userID string) string

	// S3SSEKMSEncryptionContext returns the per-tenant S3 KMS-SSE key id or an empty string if not set.
	S3SSEKMSEncryptionContext(userID string) string
}

// SSEBucketReaderClient is a wrapper around a objstore.BucketReader that configures the object
// storage server-side encryption (SSE) for a given user.
type SSEBucketReaderClient struct {
	userID string
	bucket objstore.BucketReader
}

// SSEBucketClient is a wrapper around a objstore.BucketReader that configures the object
// storage server-side encryption (SSE) for a given user.
type SSEBucketClient struct {
	SSEBucketReaderClient
	bucket      objstore.Bucket
	cfgProvider TenantConfigProvider
}

// NewSSEBucketClient makes a new SSEBucketClient. The cfgProvider can be nil.
func NewSSEBucketClient(userID string, bucket objstore.Bucket, cfgProvider TenantConfigProvider) *SSEBucketClient {
	return &SSEBucketClient{
		SSEBucketReaderClient: SSEBucketReaderClient{
			userID: userID,
			bucket: bucket,
		},
		bucket:      bucket,
		cfgProvider: cfgProvider,
	}
}

// Close implements objstore.Bucket.
func (b *SSEBucketClient) Close() error {
	return b.bucket.Close()
}

// Upload the contents of the reader as an object into the bucket.
func (b *SSEBucketClient) Upload(ctx context.Context, name string, r io.Reader) error {
	if sse, err := b.getCustomS3SSEConfig(); err != nil {
		return err
	} else if sse != nil {
		// If the underlying bucket client is not S3 and a custom S3 SSE config has been
		// provided, the config option will be ignored.
		ctx = s3.ContextWithSSEConfig(ctx, sse)
	}

	return b.bucket.Upload(ctx, name, r)
}

// Delete implements objstore.Bucket.
func (b *SSEBucketClient) Delete(ctx context.Context, name string) error {
	return b.bucket.Delete(ctx, name)
}

// Name implements objstore.Bucket.
func (b *SSEBucketClient) Name() string {
	return b.bucket.Name()
}

func (b *SSEBucketClient) getCustomS3SSEConfig() (encrypt.ServerSide, error) {
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

// Iter implements objstore.Bucket.
func (b *SSEBucketReaderClient) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return b.bucket.Iter(ctx, dir, f, options...)
}

// Get implements objstore.Bucket.
func (b *SSEBucketReaderClient) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.bucket.Get(ctx, name)
}

// GetRange implements objstore.Bucket.
func (b *SSEBucketReaderClient) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.bucket.GetRange(ctx, name, off, length)
}

// Exists implements objstore.Bucket.
func (b *SSEBucketReaderClient) Exists(ctx context.Context, name string) (bool, error) {
	return b.bucket.Exists(ctx, name)
}

// IsObjNotFoundErr implements objstore.Bucket.
func (b *SSEBucketReaderClient) IsObjNotFoundErr(err error) bool {
	return b.bucket.IsObjNotFoundErr(err)
}

// Attributes implements objstore.Bucket.
func (b *SSEBucketReaderClient) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return b.bucket.Attributes(ctx, name)
}

// ReaderWithExpectedErrs implements objstore.Bucket.
func (b *SSEBucketReaderClient) ReaderWithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	if ib, ok := b.bucket.(objstore.InstrumentedBucketReader); ok {
		return &SSEBucketReaderClient{
			userID: b.userID,
			bucket: ib.ReaderWithExpectedErrs(fn),
		}
	}

	return b
}

// WithExpectedErrs implements objstore.Bucket.
func (b *SSEBucketClient) WithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ib, ok := b.bucket.(objstore.InstrumentedBucket); ok {
		nb := ib.WithExpectedErrs(fn)

		return &SSEBucketClient{
			SSEBucketReaderClient: SSEBucketReaderClient{
				userID: b.userID,
				bucket: nb,
			},
			bucket: nb,
		}
	}

	return b
}
