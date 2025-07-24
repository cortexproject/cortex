package tenantfederation

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func Test_RegexResolver(t *testing.T) {
	tests := []struct {
		description     string
		existingTenants []string
		orgID           string
		expectedErr     error
		expectedOrgIDs  []string
		maxTenants      int
	}{
		{
			description:     "invalid regex",
			existingTenants: []string{},
			orgID:           "[a-z",
			expectedErr:     errInvalidRegex,
		},
		{
			description:     "no matched tenantID",
			existingTenants: []string{"user-1", "user-2", "user-3"},
			orgID:           "user-[4-6]",
			expectedOrgIDs:  []string{"fake"},
		},
		{
			description:     "use tenantIDsLabelSeparator",
			existingTenants: []string{"user-1", "user-2", "user-3"},
			orgID:           "user-1|user-2|user-3",
			expectedOrgIDs:  []string{"user-1", "user-2", "user-3"},
		},
		{
			description:     "use regex",
			existingTenants: []string{"user-1", "user-2", "user-3"},
			orgID:           "user-.+",
			expectedOrgIDs:  []string{"user-1", "user-2", "user-3"},
		},
		{
			description:     "newly created tenant",
			existingTenants: []string{},
			orgID:           "user-1",
			expectedOrgIDs:  []string{"user-1"},
		},
		{
			description:     "user-2 hasn't been uploaded yet",
			existingTenants: []string{"user-1"},
			orgID:           "user-1|user-2",
			expectedOrgIDs:  []string{"user-1"},
		},
		{
			description:     "adjust maxTenant",
			existingTenants: []string{"user-1", "user-2", "user-3"},
			orgID:           "user-.+",
			maxTenants:      2,
			expectedErr:     errors.New("too many tenants, max: 2, actual: 3"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("", tc.existingTenants, nil)
			bucketClient.MockIter("__markers__", []string{}, nil)
			for _, tenant := range tc.existingTenants {
				bucketClient.MockExists(cortex_tsdb.GetGlobalDeletionMarkPath(tenant), false, nil)
				bucketClient.MockExists(cortex_tsdb.GetLocalDeletionMarkPath(tenant), false, nil)
			}

			bucketClientFactory := func(ctx context.Context) (objstore.InstrumentedBucket, error) {
				return bucketClient, nil
			}

			usersScannerConfig := cortex_tsdb.UsersScannerConfig{Strategy: cortex_tsdb.UserScanStrategyList}
			tenantFederationConfig := Config{UserSyncInterval: time.Second, MaxTenant: tc.maxTenants}
			regexResolver, err := NewRegexResolver(usersScannerConfig, tenantFederationConfig, reg, bucketClientFactory, log.NewNopLogger())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), regexResolver))

			// wait update knownUsers
			test.Poll(t, time.Second*10, true, func() interface{} {
				return testutil.ToFloat64(regexResolver.lastUpdateUserRun) > 0 && testutil.ToFloat64(regexResolver.discoveredUsers) == float64(len(tc.existingTenants))
			})

			// set regexOrgID
			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, tc.orgID)
			orgIDs, err := regexResolver.TenantIDs(ctx)

			if tc.expectedErr != nil {
				require.Contains(t, err.Error(), tc.expectedErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedOrgIDs, orgIDs)
			}
		})
	}
}

func Test_RegexValidator(t *testing.T) {
	tests := []struct {
		description string
		orgID       string
		expectedErr error
	}{
		{
			description: "valid regex",
			orgID:       "user-.*",
			expectedErr: nil,
		},
		{
			description: "invalid regex",
			orgID:       "[a-z",
			expectedErr: errInvalidRegex,
		},
		{
			description: "tenant ID is too long",
			orgID:       strings.Repeat("a", 151),
			expectedErr: errors.New("tenant ID is too long: max 150 characters"),
		},
		{
			description: ".",
			orgID:       ".",
			expectedErr: errors.New("tenant ID is '.' or '..'"),
		},
		{
			description: "..",
			orgID:       "..",
			expectedErr: errors.New("tenant ID is '.' or '..'"),
		},
		{
			description: "__markers__",
			orgID:       "__markers__",
			expectedErr: errors.New("tenant ID '__markers__' is not allowed"),
		},
		{
			description: "user-index.json.gz",
			orgID:       "user-index.json.gz",
			expectedErr: errors.New("tenant ID 'user-index.json.gz' is not allowed"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			regexValidator := NewRegexValidator()
			ctx := user.InjectOrgID(context.Background(), tc.orgID)
			_, err := regexValidator.TenantID(ctx)
			require.Equal(t, tc.expectedErr, err)
		})
	}
}
