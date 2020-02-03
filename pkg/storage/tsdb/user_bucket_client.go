package tsdb

import (
	"fmt"
	"io"
	"strings"

	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/net/context"
)

// UserBucketClient is a wrapper around a objstore.Bucket that prepends writes with a userID
type UserBucketClient struct {
	userID string
	bucket objstore.Bucket
}

// NewUserBucketClient makes a new UserBucketClient.
func NewUserBucketClient(userID string, bucket objstore.Bucket) *UserBucketClient {
	return &UserBucketClient{
		userID: userID,
		bucket: bucket,
	}
}

func (b *UserBucketClient) fullName(name string) string {
	return fmt.Sprintf("%s/%s", b.userID, name)
}

// Close implements io.Closer
func (b *UserBucketClient) Close() error { return b.bucket.Close() }

// Upload the contents of the reader as an object into the bucket.
func (b *UserBucketClient) Upload(ctx context.Context, name string, r io.Reader) error {
	return b.bucket.Upload(ctx, b.fullName(name), r)
}

// Delete removes the object with the given name.
func (b *UserBucketClient) Delete(ctx context.Context, name string) error {
	return b.bucket.Delete(ctx, b.fullName(name))
}

// Name returns the bucket name for the provider.
func (b *UserBucketClient) Name() string { return b.bucket.Name() }

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *UserBucketClient) Iter(ctx context.Context, dir string, f func(string) error) error {
	return b.bucket.Iter(ctx, b.fullName(dir), func(s string) error {
		/*
			Since all objects are prefixed with the userID we need to strip the userID
			upon passing to the processing function
		*/
		return f(strings.Join(strings.Split(s, "/")[1:], "/"))
	})
}

// Get returns a reader for the given object name.
func (b *UserBucketClient) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.bucket.Get(ctx, b.fullName(name))
}

// GetRange returns a new range reader for the given object name and range.
func (b *UserBucketClient) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.bucket.GetRange(ctx, b.fullName(name), off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *UserBucketClient) Exists(ctx context.Context, name string) (bool, error) {
	return b.bucket.Exists(ctx, b.fullName(name))
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *UserBucketClient) IsObjNotFoundErr(err error) bool {
	return b.bucket.IsObjNotFoundErr(err)
}

// ObjectSize returns the size of the specified object.
func (b *UserBucketClient) ObjectSize(ctx context.Context, name string) (uint64, error) {
	return b.bucket.ObjectSize(ctx, b.fullName(name))
}
