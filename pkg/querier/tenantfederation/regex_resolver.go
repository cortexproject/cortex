package tenantfederation

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/users"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/services"
)

var (
	errInvalidRegex = errors.New("invalid regex present")

	ErrTooManyTenants = "too many tenants, max: %d, actual: %d"
)

// RegexResolver resolves tenantIDs matched given regex.
type RegexResolver struct {
	services.Service

	knownUsers       []string
	userSyncInterval time.Duration
	maxTenant        int
	userScanner      users.Scanner
	logger           log.Logger
	sync.Mutex

	// lastUpdateUserRun stores the timestamps of the latest update user loop run
	lastUpdateUserRun prometheus.Gauge
	// discoveredUsers stores the number of discovered user
	discoveredUsers prometheus.Gauge
}

func NewRegexResolver(cfg tsdb.UsersScannerConfig, tenantFederationCfg Config, reg prometheus.Registerer, bucketClientFactory func(ctx context.Context) (objstore.InstrumentedBucket, error), logger log.Logger) (*RegexResolver, error) {
	bucketClient, err := bucketClientFactory(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the bucket client")
	}

	userScanner, err := users.NewScanner(cfg, bucketClient, logger, extprom.WrapRegistererWith(prometheus.Labels{"component": "regex-resolver"}, reg))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create users scanner")
	}

	r := &RegexResolver{
		userSyncInterval: tenantFederationCfg.UserSyncInterval,
		maxTenant:        tenantFederationCfg.MaxTenant,
		userScanner:      userScanner,
		logger:           logger,
	}

	r.lastUpdateUserRun = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_regex_resolver_last_update_run_timestamp_seconds",
		Help: "Unix timestamp of the last successful regex resolver update user run.",
	})
	r.discoveredUsers = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_regex_resolver_discovered_users",
		Help: "Number of discovered users.",
	})

	r.Service = services.NewBasicService(nil, r.running, nil)

	return r, nil
}

func (r *RegexResolver) running(ctx context.Context) error {
	level.Info(r.logger).Log("msg", "regex-resolver started")
	ticker := time.NewTicker(r.userSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// active and deleting users are considered
			// The store-gateway can query for deleting users.
			active, deleting, _, err := r.userScanner.ScanUsers(ctx)
			if err != nil {
				level.Error(r.logger).Log("msg", "failed to discover users from bucket", "err", err)
			}

			r.Lock()
			r.knownUsers = append(active, deleting...)
			// We keep it sort
			sort.Strings(r.knownUsers)
			r.Unlock()
			r.lastUpdateUserRun.SetToCurrentTime()
			r.discoveredUsers.Set(float64(len(active) + len(deleting)))
		}
	}
}

func (r *RegexResolver) TenantID(ctx context.Context) (string, error) {
	orgIDs, err := r.TenantIDs(ctx)
	if err != nil {
		return "", err
	}

	if len(orgIDs) > 1 {
		return "", user.ErrTooManyOrgIDs
	}

	return orgIDs[0], nil
}

func (r *RegexResolver) TenantIDs(ctx context.Context) ([]string, error) {
	//lint:ignore faillint wrapper around upstream method
	orgID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	orgIDs, err := r.getRegexMatchedOrgIds(orgID)
	if err != nil {
		return nil, err
	}

	return tenant.ValidateOrgIDs(orgIDs)
}

func (r *RegexResolver) getRegexMatchedOrgIds(orgID string) ([]string, error) {
	var matched []string

	// Use the Prometheus FastRegexMatcher
	m, err := labels.NewFastRegexMatcher(orgID)
	if err != nil {
		return nil, errInvalidRegex
	}

	r.Lock()
	defer r.Unlock()
	for _, id := range r.knownUsers {
		if m.MatchString(id) {
			matched = append(matched, id)
		}
	}

	if len(matched) == 0 {
		if err := tenant.ValidTenantID(orgID); err == nil {
			// when querying for a newly created orgID, the query may not
			// work because it has not been uploaded to object storage.
			// To make the query work (not breaking existing behavior),
			// paas the orgID if it is valid.
			return []string{orgID}, nil
		}

		// when the entered regex is an invalid tenantID,
		// set the `fake` to `X-Scope-OrgID`.
		return []string{"fake"}, nil
	}

	if r.maxTenant > 0 && len(matched) > r.maxTenant {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", fmt.Errorf(ErrTooManyTenants, r.maxTenant, len(matched)).Error())
	}

	return matched, nil
}

// RegexValidator used to pass a regex orgID to the querier.
// Using an existing tenant resolver could emit an errTenantIDUnsupportedCharacter
// since the regex would contain unsupported characters like a `+`.
type RegexValidator struct{}

func NewRegexValidator() *RegexValidator {
	return &RegexValidator{}
}

func (r *RegexValidator) TenantID(ctx context.Context) (string, error) {
	//lint:ignore faillint wrapper around upstream method
	id, err := user.ExtractOrgID(ctx)
	if err != nil {
		return "", err
	}

	_, err = labels.NewFastRegexMatcher(id)
	if err != nil {
		return "", errInvalidRegex
	}

	if err := tenant.CheckTenantIDLength(id); err != nil {
		return "", err
	}

	if err := tenant.CheckTenantIDIsSupported(id); err != nil {
		return "", err
	}

	return id, nil
}

func (r *RegexValidator) TenantIDs(ctx context.Context) ([]string, error) {
	orgID, err := r.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	return []string{orgID}, nil
}
