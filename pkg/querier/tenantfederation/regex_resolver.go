package tenantfederation

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/tenant"
)

var (
	errInvalidRegex = errors.New("invalid regex present")

	allUserStatTimeout = time.Second * 5
)

type RegexResolver struct {
	allUserStatFunc func(ctx context.Context) ([]ingester.UserIDStats, error)
	knownUsers      []string
	logger          log.Logger
	sync.Mutex

	// lastUpdateUserRun stores the timestamps of the latest update user loop run
	lastUpdateUserRun prometheus.Gauge
}

func NewRegexResolver(reg prometheus.Registerer, userSyncInterval time.Duration, logger log.Logger, allUserStatFunc func(ctx context.Context) ([]ingester.UserIDStats, error)) *RegexResolver {
	r := &RegexResolver{
		allUserStatFunc: allUserStatFunc,
		logger:          logger,
	}

	r.lastUpdateUserRun = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_regex_resolver_last_update_run_timestamp_seconds",
		Help: "Unix timestamp of the last successful regex resolver update user run.",
	})

	go r.updateUsersLoop(userSyncInterval, allUserStatTimeout)
	return r
}

func (r *RegexResolver) updateUsersLoop(interval time.Duration, allUserStatTimeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	updateFunc := func() {
		ctx, cancel := context.WithTimeout(context.Background(), allUserStatTimeout)
		defer cancel()
		stats, err := r.allUserStatFunc(ctx)
		if err != nil {
			level.Error(r.logger).Log("msg", "error getting user stats", "err", err)
			return
		}

		users := make([]string, 0, len(stats))
		for _, stat := range stats {
			users = append(users, stat.UserID)
		}

		r.Lock()
		r.knownUsers = users
		// We keep it sort
		sort.Strings(r.knownUsers)
		r.Unlock()
		r.lastUpdateUserRun.SetToCurrentTime()
	}

	updateFunc()
	for range ticker.C {
		updateFunc()
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
		// The entered regex could be invalid tenantID, so if there is
		// nothing to match, set the `fake` to `X-Scope-OrgID`.
		return []string{"fake"}, nil
	}

	return matched, nil
}

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

	return id, nil
}

func (r *RegexValidator) TenantIDs(ctx context.Context) ([]string, error) {
	orgID, err := r.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	return []string{orgID}, nil
}
