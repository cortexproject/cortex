package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestBucketWithRetries_UploadSeekable(t *testing.T) {
	t.Parallel()

	m := mockBucket{
		FailCount: 3,
	}
	b := BucketWithRetries{
		logger:           log.NewNopLogger(),
		bucket:           &m,
		operationRetries: 5,
		retryMinBackoff:  10 * time.Millisecond,
		retryMaxBackoff:  time.Second,
	}

	input := []byte("test input")
	err := b.Upload(context.Background(), "dummy", bytes.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, input, m.uploadedContent)
}

func TestBucketWithRetries_UploadNonSeekable(t *testing.T) {
	t.Parallel()

	maxFailCount := 3
	m := mockBucket{
		FailCount: maxFailCount,
	}
	b := BucketWithRetries{
		logger:           log.NewNopLogger(),
		bucket:           &m,
		operationRetries: 5,
		retryMinBackoff:  10 * time.Millisecond,
		retryMaxBackoff:  time.Second,
	}

	input := &fakeReader{}
	err := b.Upload(context.Background(), "dummy", input)
	require.Errorf(t, err, "empty byte slice")
	require.Equal(t, maxFailCount, m.FailCount)
}

func TestBucketWithRetries_UploadFailed(t *testing.T) {
	t.Parallel()

	m := mockBucket{
		FailCount: 6,
	}
	b := BucketWithRetries{
		logger:           log.NewNopLogger(),
		bucket:           &m,
		operationRetries: 5,
		retryMinBackoff:  10 * time.Millisecond,
		retryMaxBackoff:  time.Second,
	}

	input := []byte("test input")
	err := b.Upload(context.Background(), "dummy", bytes.NewReader(input))
	require.ErrorContains(t, err, "failed upload: ")
}

func TestBucketWithRetries_ContextCanceled(t *testing.T) {
	t.Parallel()

	m := mockBucket{}
	b := BucketWithRetries{
		logger:           log.NewNopLogger(),
		bucket:           &m,
		operationRetries: 5,
		retryMinBackoff:  10 * time.Millisecond,
		retryMaxBackoff:  time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	obj, err := b.GetRange(ctx, "dummy", 0, 10)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, obj)
}

type fakeReader struct {
}

func (f *fakeReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("empty byte slice")
}

type mockBucket struct {
	FailCount       int
	uploadedContent []byte
}

// Upload mocks objstore.Bucket.Upload()
func (m *mockBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		return err
	}
	m.uploadedContent = buf.Bytes()
	if m.FailCount > 0 {
		m.FailCount--
		return fmt.Errorf("failed upload: %d", m.FailCount)
	}
	return nil
}

// Delete mocks objstore.Bucket.Delete()
func (m *mockBucket) Delete(ctx context.Context, name string) error {
	return nil
}

// Name mocks objstore.Bucket.Name()
func (m *mockBucket) Name() string {
	return "mock"
}

// Iter mocks objstore.Bucket.Iter()
func (m *mockBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return nil
}

// Get mocks objstore.Bucket.Get()
func (m *mockBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return nil, nil
}

// GetRange mocks objstore.Bucket.GetRange()
func (m *mockBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewBuffer(bytes.Repeat([]byte{1}, int(length)))), nil
}

// Exists mocks objstore.Bucket.Exists()
func (m *mockBucket) Exists(ctx context.Context, name string) (bool, error) {
	return false, nil
}

// IsObjNotFoundErr mocks objstore.Bucket.IsObjNotFoundErr()
func (m *mockBucket) IsObjNotFoundErr(err error) bool {
	return false
}

// ObjectSize mocks objstore.Bucket.Attributes()
func (m *mockBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return objstore.ObjectAttributes{Size: 0, LastModified: time.Now()}, nil
}

// Close mocks objstore.Bucket.Close()
func (m *mockBucket) Close() error {
	return nil
}
