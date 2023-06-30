package testutil

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/util"

	"github.com/cortexproject/cortex/pkg/storage/bucket/filesystem"
)

var ErrKeyAccessDeniedError = errors.New("test key access denied")

func PrepareFilesystemBucket(t testing.TB) (objstore.Bucket, string) {
	storageDir, err := os.MkdirTemp(os.TempDir(), "bucket")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(storageDir))
	})

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	return objstore.BucketWithMetrics("test", bkt, nil), storageDir
}

type MockBucketFailure struct {
	objstore.Bucket

	DeleteFailures []string
	GetFailures    map[string]error
}

func (m *MockBucketFailure) Delete(ctx context.Context, name string) error {
	if util.StringsContain(m.DeleteFailures, name) {
		return errors.New("mocked delete failure")
	}
	return m.Bucket.Delete(ctx, name)
}

func (m *MockBucketFailure) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	for prefix, err := range m.GetFailures {
		if strings.HasPrefix(name, prefix) {
			return nil, err
		}
	}
	if e, ok := m.GetFailures[name]; ok {
		return nil, e
	}

	return m.Bucket.Get(ctx, name)
}

func (m *MockBucketFailure) WithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ibkt, ok := m.Bucket.(objstore.InstrumentedBucket); ok {
		return &MockBucketFailure{Bucket: ibkt.WithExpectedErrs(expectedFunc), DeleteFailures: m.DeleteFailures, GetFailures: m.GetFailures}
	}

	return m
}

func (m *MockBucketFailure) ReaderWithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	if ibkt, ok := m.Bucket.(objstore.InstrumentedBucket); ok {
		return &MockBucketFailure{Bucket: ibkt.WithExpectedErrs(expectedFunc), DeleteFailures: m.DeleteFailures, GetFailures: m.GetFailures}
	}

	return m
}

func (m *MockBucketFailure) IsCustomerManagedKeyError(err error) bool {
	return errors.Is(err, ErrKeyAccessDeniedError)
}
