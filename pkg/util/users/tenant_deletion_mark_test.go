package users

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/util/testutil"
)

func TestTenantDeletionMarkExists(t *testing.T) {
	const username = "user"

	for name, tc := range map[string]struct {
		objects      map[string][]byte
		exists       bool
		deletedUsers []string
	}{
		"empty": {
			objects: nil,
			exists:  false,
		},

		"mark doesn't exist": {
			objects: map[string][]byte{
				"user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json": []byte("data"),
			},
			exists: false,
		},

		"local mark exists": {
			objects: map[string][]byte{
				"user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json": []byte("data"),
				GetLocalDeletionMarkPath("user"):            []byte("data"),
			},
			exists: true,
		},
		"global mark exists": {
			objects: map[string][]byte{
				"user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json": []byte("data"),
				GetGlobalDeletionMarkPath("user"):           []byte("data"),
			},
			exists: true,
		},
		"mark exists - upload via WriteTenantDeletionMark": {
			objects: map[string][]byte{
				"user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json": []byte("data"),
			},
			deletedUsers: []string{"user"},
			exists:       true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			// "upload" objects
			for objName, data := range tc.objects {
				require.NoError(t, bkt.Upload(context.Background(), objName, bytes.NewReader(data)))
			}

			for _, user := range tc.deletedUsers {
				require.NoError(t, WriteTenantDeletionMark(context.Background(), objstore.WithNoopInstr(bkt), user, &TenantDeletionMark{}))
			}

			res, err := TenantDeletionMarkExists(context.Background(), bkt, username)
			require.NoError(t, err)
			require.Equal(t, tc.exists, res)
		})
	}
}

func TestDeleteTenantDeletionMark(t *testing.T) {
	const username = "user"

	for name, tc := range map[string]struct {
		objects        []string
		deleteFailures []string
		expectedErr    string
	}{
		"only global mark exists": {
			objects: []string{GetGlobalDeletionMarkPath(username)},
		},
		"only local mark exists": {
			objects: []string{GetLocalDeletionMarkPath(username)},
		},
		"both marks exist": {
			objects: []string{GetGlobalDeletionMarkPath(username), GetLocalDeletionMarkPath(username)},
		},
		"no mark exists": {
			objects: nil,
		},
		"failure deleting global mark": {
			objects:        []string{GetGlobalDeletionMarkPath(username)},
			deleteFailures: []string{GetGlobalDeletionMarkPath(username)},
			expectedErr:    "mocked delete failure",
		},
		"failure deleting local mark": {
			objects:        []string{GetGlobalDeletionMarkPath(username), GetLocalDeletionMarkPath(username)},
			deleteFailures: []string{GetLocalDeletionMarkPath(username)},
			expectedErr:    "mocked delete failure",
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Like GCS, Azure, Swift and OCI, the in-memory bucket returns an error when deleting a missing object.
			bkt := objstore.NewInMemBucket()
			for _, objName := range tc.objects {
				require.NoError(t, bkt.Upload(context.Background(), objName, bytes.NewReader([]byte("data"))))
			}

			err := DeleteTenantDeletionMark(context.Background(), &testutil.MockBucketFailure{Bucket: bkt, DeleteFailures: tc.deleteFailures}, username)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)

			exists, err := TenantDeletionMarkExists(context.Background(), bkt, username)
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}
