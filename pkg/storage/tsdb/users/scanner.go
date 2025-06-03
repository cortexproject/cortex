package users

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/tenant"
)

type Scanner interface {
	// ScanUsers returns the list of active, deleting and deleted users.
	// Both deleting and deleted users are marked for deletion. The difference is that
	// deleting users might still have data in the bucket, while deleted users don't.
	ScanUsers(ctx context.Context) (active, deleting, deleted []string, err error)
}

func NewScanner(cfg tsdb.UsersScannerConfig, bkt objstore.InstrumentedBucket, logger log.Logger) (Scanner, error) {
	var scanner Scanner
	switch cfg.Strategy {
	case tsdb.UserScanStrategyList:
		scanner = &listScanner{bkt: bkt}
	case tsdb.UserScanStrategyUserIndex:
		scanner = &userIndexScanner{
			bkt:            bkt,
			logger:         logger,
			baseScanner:    &listScanner{bkt: bkt},
			maxStalePeriod: cfg.MaxStalePeriod,
		}
	default:
		return nil, tsdb.ErrInvalidUserScannerStrategy
	}

	if cfg.CacheTTL > 0 {
		scanner = &cachedScanner{
			scanner: scanner,
			ttl:     cfg.CacheTTL,
		}
	}

	return scanner, nil
}

func NewShardedScanner(scanner Scanner, isOwned func(userID string) (bool, error), logger log.Logger) Scanner {
	return &shardedScanner{
		scanner: scanner,
		isOwned: isOwned,
		logger:  logger,
	}
}

type listScanner struct {
	bkt objstore.InstrumentedBucket
}

func (s *listScanner) ScanUsers(ctx context.Context) (active, deleting, deleted []string, err error) {
	scannedActiveUsers := make(map[string]struct{})
	scannedMarkedForDeletionUsers := make(map[string]struct{})
	deletingUsers := make(map[string]struct{})

	// Scan users in the bucket.
	err = s.bkt.Iter(ctx, "", func(entry string) error {
		userID := strings.TrimSuffix(entry, "/")
		if userID == tenant.GlobalMarkersDir {
			return nil
		}
		scannedActiveUsers[userID] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// Scan users from the __markers__ directory.
	err = s.bkt.Iter(ctx, tenant.GlobalMarkersDir, func(entry string) error {
		// entry will be of the form __markers__/<user>/
		parts := strings.Split(entry, objstore.DirDelim)
		userID := parts[1]
		scannedMarkedForDeletionUsers[userID] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	for userID := range scannedActiveUsers {
		// Tenant deletion mark could exist in local path for legacy code.
		// If tenant deletion mark exists but user ID prefix exists in the bucket, mark it as deleting.
		if deletionMarkExists, err := tsdb.TenantDeletionMarkExists(ctx, s.bkt, userID); err == nil && deletionMarkExists {
			deletingUsers[userID] = struct{}{}
			continue
		}

		active = append(active, userID)
	}

	for userID := range scannedMarkedForDeletionUsers {
		// User marked for deletion but no user ID prefix in the bucket, mark it as deleted.
		if _, ok := deletingUsers[userID]; !ok {
			deleted = append(deleted, userID)
		}
	}

	for userID := range deletingUsers {
		deleting = append(deleting, userID)
	}

	sort.Strings(active)
	sort.Strings(deleting)
	sort.Strings(deleted)
	return active, deleting, deleted, nil
}

type userIndexScanner struct {
	bkt         objstore.InstrumentedBucket
	logger      log.Logger
	baseScanner Scanner

	// Maximum period of time to consider the user index as stale.
	maxStalePeriod time.Duration
}

func (s *userIndexScanner) ScanUsers(ctx context.Context) ([]string, []string, []string, error) {
	userIndex, err := ReadUserIndex(ctx, s.bkt, s.logger)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			level.Info(s.logger).Log("msg", "user index not found, fallback to base scanner")
		} else {
			// Always fallback to the list scanner if failed to read the user index.
			level.Error(s.logger).Log("msg", "failed to read user index, fallback to base scanner", "error", err)
		}
		return s.baseScanner.ScanUsers(ctx)
	}

	if userIndex.GetUpdatedAt().Before(time.Now().Add(-s.maxStalePeriod)) {
		level.Warn(s.logger).Log("msg", "user index is stale, fallback to base scanner", "updated_at", userIndex.GetUpdatedAt(), "max_stale_period", s.maxStalePeriod)
		return s.baseScanner.ScanUsers(ctx)
	}

	return userIndex.ActiveUsers, userIndex.DeletingUsers, userIndex.DeletedUsers, nil
}

// shardedScanner is a user scanner but applies a filter to the users to check ownership.
type shardedScanner struct {
	scanner Scanner
	isOwned func(userID string) (bool, error)
	logger  log.Logger
}

func (s *shardedScanner) ScanUsers(ctx context.Context) ([]string, []string, []string, error) {
	baseActiveUsers, baseDeletingUsers, baseDeletedUsers, err := s.scanner.ScanUsers(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	activeUsers := make([]string, 0, len(baseActiveUsers))
	deletingUsers := make([]string, 0, len(baseDeletingUsers))
	deletedUsers := make([]string, 0, len(baseDeletedUsers))

	for _, userID := range baseActiveUsers {
		// Filter out users not owned by this instance.
		if owned, err := s.isOwned(userID); err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is owned by this shard", "user", userID, "err", err)
			continue
		} else if !owned {
			continue
		}
		activeUsers = append(activeUsers, userID)
	}
	for _, userID := range baseDeletingUsers {
		// Filter out users not owned by this instance.
		if owned, err := s.isOwned(userID); err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is owned by this shard", "user", userID, "err", err)
			continue
		} else if !owned {
			continue
		}
		deletingUsers = append(deletingUsers, userID)
	}
	for _, userID := range baseDeletedUsers {
		// Filter out users not owned by this instance.
		if owned, err := s.isOwned(userID); err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is owned by this shard", "user", userID, "err", err)
			continue
		} else if !owned {
			continue
		}
		deletedUsers = append(deletedUsers, userID)
	}

	return activeUsers, deletingUsers, deletedUsers, nil
}
