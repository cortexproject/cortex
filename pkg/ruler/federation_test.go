package ruler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFederatedRulesChecker_CheckOwner(t *testing.T) {
	tests := map[string]struct {
		cfg         Config
		userID      string
		expectedErr error
	}{
		"disabled": {
			cfg:         Config{},
			userID:      "infra",
			expectedErr: errFederatedRulesDisabled,
		},
		"enabled for all tenants": {
			cfg:    Config{EnableFederatedRules: true},
			userID: "infra",
		},
		"allowed tenant": {
			cfg:    Config{EnableFederatedRules: true, AllowedFederatedTenants: []string{"infra"}},
			userID: "infra",
		},
		"tenant not in allowed list": {
			cfg:         Config{EnableFederatedRules: true, AllowedFederatedTenants: []string{"infra"}},
			userID:      "team-a",
			expectedErr: errFederatedRulesNotAllowed,
		},
		"disallowed tenant": {
			cfg:         Config{EnableFederatedRules: true, DisallowedFederatedTenants: []string{"team-a"}},
			userID:      "team-a",
			expectedErr: errFederatedRulesNotAllowed,
		},
		"allowed and disallowed": {
			cfg:         Config{EnableFederatedRules: true, AllowedFederatedTenants: []string{"infra"}, DisallowedFederatedTenants: []string{"infra"}},
			userID:      "infra",
			expectedErr: errFederatedRulesNotAllowed,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := newFederatedRulesChecker(tc.cfg).checkOwner(tc.userID)
			if tc.expectedErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestFederatedRulesChecker_ValidateSourceTenants(t *testing.T) {
	tests := map[string]struct {
		cfg           Config
		sourceTenants []string
		expected      []string
		expectedErr   string
	}{
		"sorted and de-duplicated": {
			sourceTenants: []string{"team-b", "team-a", "team-b"},
			expected:      []string{"team-a", "team-b"},
		},
		"invalid tenant id": {
			sourceTenants: []string{"team-a", "team|b"},
			expectedErr:   `invalid source tenant "team|b"`,
		},
		"empty tenant id": {
			sourceTenants: []string{""},
			expectedErr:   "source tenant must not be empty",
		},
		"empty tenant id among valid ones": {
			sourceTenants: []string{"team-a", ""},
			expectedErr:   "source tenant must not be empty",
		},
		"regex metacharacters allowed without regex matcher": {
			sourceTenants: []string{"team.a"},
			expected:      []string{"team.a"},
		},
		"regex metacharacters rejected with regex matcher": {
			cfg:           Config{TenantFederationRegexMatcherEnabled: true},
			sourceTenants: []string{"team.a"},
			expectedErr:   `source tenant "team.a" contains regex metacharacters`,
		},
		"max tenant": {
			cfg:           Config{TenantFederationMaxTenant: 2},
			sourceTenants: []string{"team-a", "team-b", "team-c"},
			expectedErr:   "too many source tenants (limit: 2 actual: 3)",
		},
		"max tenant counts unique tenants": {
			cfg:           Config{TenantFederationMaxTenant: 2},
			sourceTenants: []string{"team-a", "team-b", "team-a"},
			expected:      []string{"team-a", "team-b"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			input := append([]string(nil), tc.sourceTenants...)
			actual, err := newFederatedRulesChecker(tc.cfg).validateSourceTenants(input)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
			// The input must not be modified.
			require.Equal(t, tc.sourceTenants, input)
		})
	}
}
