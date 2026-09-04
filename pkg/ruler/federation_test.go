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

func TestFederatedRulesChecker_ValidateSrcTenants(t *testing.T) {
	tests := map[string]struct {
		cfg         Config
		srcTenants  []string
		expected    []string
		expectedErr string
	}{
		"sorted and de-duplicated": {
			srcTenants: []string{"team-b", "team-a", "team-b"},
			expected:   []string{"team-a", "team-b"},
		},
		"invalid tenant id": {
			srcTenants:  []string{"team-a", "team|b"},
			expectedErr: `invalid src tenant "team|b"`,
		},
		"empty tenant id": {
			srcTenants:  []string{""},
			expectedErr: "src tenant must not be empty",
		},
		"empty tenant id among valid ones": {
			srcTenants:  []string{"team-a", ""},
			expectedErr: "src tenant must not be empty",
		},
		"regex metacharacters allowed without regex matcher": {
			srcTenants: []string{"team.a"},
			expected:   []string{"team.a"},
		},
		"regex metacharacters rejected with regex matcher": {
			cfg:         Config{TenantFederationRegexMatcherEnabled: true},
			srcTenants:  []string{"team.a"},
			expectedErr: `src tenant "team.a" contains regex metacharacters`,
		},
		"max tenant": {
			cfg:         Config{TenantFederationMaxTenant: 2},
			srcTenants:  []string{"team-a", "team-b", "team-c"},
			expectedErr: "too many src tenants (limit: 2 actual: 3)",
		},
		"max tenant counts unique tenants": {
			cfg:        Config{TenantFederationMaxTenant: 2},
			srcTenants: []string{"team-a", "team-b", "team-a"},
			expected:   []string{"team-a", "team-b"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			input := append([]string(nil), tc.srcTenants...)
			actual, err := newFederatedRulesChecker(tc.cfg).validateSrcTenants(input)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
			// The input must not be modified.
			require.Equal(t, tc.srcTenants, input)
		})
	}
}
