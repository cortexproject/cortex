package tsdb

import (
	"context"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/time/rate"
)

func mockVisit(string) error {
	return nil
}

func TestRateLimitingBucket(t *testing.T) {
	ctx := context.Background()
	mb := MockBucket{}
	// Allow 4 requests per second with a burst of 1
	rlb := NewRateLimitingBucket(mb, rate.Limit(4), 1)

	// Check the rate limit works for Iter
	assert.NoError(t, rlb.Iter(ctx, "some_dir", mockVisit))
	time.Sleep(time.Millisecond * 100)
	assert.Error(t, ErrRateLimited, rlb.Iter(ctx, "some_dir", mockVisit))
	time.Sleep(time.Millisecond * 200)
	assert.NoError(t, rlb.Iter(ctx, "some_dir", mockVisit))

	// Check the other methods are just passed along
	rdr, err := rlb.Get(ctx, "name")
	assert.NoError(t, err)
	bts, err := ioutil.ReadAll(rdr)
	assert.NoError(t, err)
	assert.Equal(t, "some data", string(bts))

	rdr, err = rlb.GetRange(ctx, "name", 100, 200)
	assert.NoError(t, err)
	bts, err = ioutil.ReadAll(rdr)
	assert.NoError(t, err)
	assert.Equal(t, "some more data", string(bts))

	ex, err := rlb.Exists(ctx, "name")
	assert.NoError(t, err)
	assert.True(t, ex)

	attr, err := rlb.Attributes(ctx, "name")
	assert.NoError(t, err)
	assert.Zero(t, attr)

	assert.True(t, rlb.IsObjNotFoundErr(ErrRateLimited))
	assert.NoError(t, rlb.Upload(ctx, "name", strings.NewReader("data")))
	assert.NoError(t, rlb.Delete(ctx, "name"))
	assert.Equal(t, rlb.Name(), "mock-bucket")
	assert.NoError(t, rlb.Close())
}

type MockBucket struct{}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (rlb MockBucket) Iter(ctx context.Context, dir string, f func(string) error) (err error) {
	return nil
}

// Get returns a reader for the given object name.
func (rlb MockBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader("some data")), nil
}

// GetRange returns a new range reader for the given object name and range.
func (rlb MockBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader("some more data")), nil
}

// Exists checks if the given object exists in the bucket.
func (rlb MockBucket) Exists(ctx context.Context, name string) (exists bool, err error) {
	return true, nil
}

// Attributes returns information about the specified object.
func (rlb MockBucket) Attributes(ctx context.Context, name string) (attrs objstore.ObjectAttributes, err error) {
	return objstore.ObjectAttributes{}, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (rlb MockBucket) IsObjNotFoundErr(err error) bool {
	return true
}

// Upload the contents of the reader as an object into the bucket.
// Upload should be idempotent.
func (rlb MockBucket) Upload(ctx context.Context, name string, r io.Reader) (err error) {
	return nil
}

// Delete removes the object with the given name.
// If object does not exists in the moment of deletion, Delete should throw error.
func (rlb MockBucket) Delete(ctx context.Context, name string) (err error) {
	return nil
}

// Name returns the bucket name for the provider.
func (rlb MockBucket) Name() string {
	return "mock-bucket"
}

// Close closes the bucket
func (rlb MockBucket) Close() error {
	return nil
}
