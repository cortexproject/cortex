package tenant

import (
	"context"
	"net/http"
	"strings"

	"github.com/weaveworks/common/user"
)

var defaultResolver Resolver = NewSingleResolver()

func DefaultResolver() Resolver {
	return defaultResolver
}

func WithDefaultResolver(r Resolver) {
	defaultResolver = r
}

type Resolver interface {
	// UserID extracts the user identifier from the context. This should be
	// used to identify a user in log messages, metrics, fairness behaviour and
	// for config overrides.
	UserID(context.Context) (string, error)

	// TenantID returns exactly a single tenant ID from the context. It should
	// be used when a certain endpoint should only support exactly a single
	// tenant ID. It fails when there is no tenant ID supplied.
	TenantID(context.Context) (string, error)

	// TenantIDs returns potentially multiple tenant IDs from the context. It
	// should be used if a supply of multiple tenant IDs is expected.
	TenantIDs(context.Context) ([]string, error)
}

// NewSingleResolver creates a tenant resolver, which restricts all requests to
// be using a single tenant only. This allows a wider set of characters to be
// used within the tenant ID and should not impose a breaking change.
func NewSingleResolver() *SingleResolver {
	return &SingleResolver{}
}

type SingleResolver struct {
}

func (t *SingleResolver) UserID(ctx context.Context) (string, error) {
	//lint:ignore faillint wrapper around upstream method
	return user.ExtractOrgID(ctx)
}

func (t *SingleResolver) TenantID(ctx context.Context) (string, error) {
	//lint:ignore faillint wrapper around upstream method
	return user.ExtractOrgID(ctx)
}

func (t *SingleResolver) TenantIDs(ctx context.Context) ([]string, error) {
	//lint:ignore faillint wrapper around upstream method
	orgID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}
	return []string{orgID}, err
}

type MultiResolver struct {
}

// NewMultiResolver creates a tenant resolver, which allows request to have
// multiple tenant ids submitted sepearted by a '|' character. This enforces
// further limits on the character set allowed within tenants as detailed here:
// https://cortexmetrics.io/docs/guides/limitations/#tenant-id-naming)
func NewMultiResolver() *MultiResolver {
	return &MultiResolver{}
}

func (t *MultiResolver) UserID(ctx context.Context) (string, error) {
	orgIDs, err := t.TenantIDs(ctx)
	if err != nil {
		return "", err
	}

	return strings.Join(orgIDs, tenantIDsLabelSeparator), nil
}

func (t *MultiResolver) TenantID(ctx context.Context) (string, error) {
	orgIDs, err := t.TenantIDs(ctx)
	if err != nil {
		return "", err
	}

	if len(orgIDs) > 1 {
		return "", user.ErrTooManyOrgIDs
	}

	return orgIDs[0], nil
}

func (t *MultiResolver) TenantIDs(ctx context.Context) ([]string, error) {
	//lint:ignore faillint wrapper around upstream method
	orgID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	orgIDs := strings.Split(orgID, tenantIDsLabelSeparator)
	for _, orgID := range orgIDs {
		if err := ValidTenantID(orgID); err != nil {
			return nil, err
		}
	}

	return NormalizeTenantIDs(orgIDs), nil
}

// ExtractTenantIDFromHTTPRequest extracts a single TenantID through a given
// resolver directly from a HTTP request.
func ExtractTenantIDFromHTTPRequest(resolver Resolver, req *http.Request) (string, context.Context, error) {
	//lint:ignore faillint wrapper around upstream method
	_, ctx, err := user.ExtractOrgIDFromHTTPRequest(req)
	if err != nil {
		return "", nil, err
	}

	tenantID, err := resolver.TenantID(ctx)
	if err != nil {
		return "", nil, err
	}

	return tenantID, ctx, nil
}
