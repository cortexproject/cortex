package backend

import (
	"fmt"
	"io"
	"strings"

	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/net/context"
)

// UserBucket is a wrapper around a objstore.Bucket that prepends writes with a userID
type UserBucket struct {
	UserID string
	Bucket objstore.Bucket
}

func (b *UserBucket) fullName(name string) string {
	return fmt.Sprintf("%s/%s", b.UserID, name)
}

// Close implements io.Closer
func (b *UserBucket) Close() error { return b.Bucket.Close() }

// Upload the contents of the reader as an object into the bucket.
func (b *UserBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	return b.Bucket.Upload(ctx, b.fullName(name), r)
}

// Delete removes the object with the given name.
func (b *UserBucket) Delete(ctx context.Context, name string) error {
	return b.Bucket.Delete(ctx, b.fullName(name))
}

// Name returns the bucket name for the provider.
func (b *UserBucket) Name() string { return b.Bucket.Name() }

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *UserBucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	return b.Bucket.Iter(ctx, b.fullName(dir), func(s string) error {
		/*
			Since all objects are prefixed with the userID we need to strip the userID
			upon passing to the processing function
		*/
		return f(strings.Join(strings.Split(s, "/")[1:], "/"))
	})
}

// Get returns a reader for the given object name.
func (b *UserBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.Bucket.Get(ctx, b.fullName(name))
}

// GetRange returns a new range reader for the given object name and range.
func (b *UserBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.Bucket.GetRange(ctx, b.fullName(name), off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *UserBucket) Exists(ctx context.Context, name string) (bool, error) {
	return b.Bucket.Exists(ctx, b.fullName(name))
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *UserBucket) IsObjNotFoundErr(err error) bool {
	return b.Bucket.IsObjNotFoundErr(err)
}
