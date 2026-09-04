package ruler

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/util/users"
)

var (
	errFederatedRulesDisabled   = errors.New("federated rules are disabled")
	errFederatedRulesNotAllowed = errors.New("tenant is not allowed to create federated rule groups")
)

// federatedRulesChecker decides whether a tenant may own federated rule groups
// and validates their source tenants.
type federatedRulesChecker struct {
	enabled             bool
	allowedTenants      *users.AllowedTenants
	regexMatcherEnabled bool
	maxTenant           int
}

func newFederatedRulesChecker(cfg Config) *federatedRulesChecker {
	return &federatedRulesChecker{
		enabled:             cfg.EnableFederatedRules,
		allowedTenants:      users.NewAllowedTenants(cfg.AllowedFederatedTenants, cfg.DisallowedFederatedTenants),
		regexMatcherEnabled: cfg.TenantFederationRegexMatcherEnabled,
		maxTenant:           cfg.TenantFederationMaxTenant,
	}
}

// checkOwner returns an error if userID may not own federated rule groups.
func (c *federatedRulesChecker) checkOwner(userID string) error {
	if !c.enabled {
		return errFederatedRulesDisabled
	}
	if !c.allowedTenants.IsAllowed(userID) {
		return fmt.Errorf("%w: %s", errFederatedRulesNotAllowed, userID)
	}
	return nil
}

// validateSrcTenants validates the source tenants of a rule group and returns
// them sorted and de-duplicated.
func (c *federatedRulesChecker) validateSrcTenants(srcTenants []string) ([]string, error) {
	for _, id := range srcTenants {
		// ValidTenantID accepts the empty string, which would produce an org ID
		// that fails at every evaluation.
		if id == "" {
			return nil, errors.New("src tenant must not be empty")
		}
		if err := users.ValidTenantID(id); err != nil {
			return nil, errors.Wrapf(err, "invalid src tenant %q", id)
		}
		// The querier interprets the joined org ID as a regex when the regex
		// matcher is enabled, so only literal tenant IDs are accepted then.
		if c.regexMatcherEnabled && regexp.QuoteMeta(id) != id {
			return nil, fmt.Errorf("src tenant %q contains regex metacharacters, which are not supported when -tenant-federation.regex-matcher-enabled is set", id)
		}
	}

	normalized := users.NormalizeTenantIDs(slices.Clone(srcTenants))
	if c.maxTenant > 0 && len(normalized) > c.maxTenant {
		return nil, fmt.Errorf("too many src tenants (limit: %d actual: %d)", c.maxTenant, len(normalized))
	}
	return normalized, nil
}
