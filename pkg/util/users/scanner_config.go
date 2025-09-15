package users

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type UsersScannerConfig struct {
	Strategy        string        `yaml:"strategy"`
	MaxStalePeriod  time.Duration `yaml:"max_stale_period"`
	CleanUpInterval time.Duration `yaml:"clean_up_interval"`
	CacheTTL        time.Duration `yaml:"cache_ttl"`
}

const (
	UserScanStrategyList      = "list"
	UserScanStrategyUserIndex = "user_index"

	defaultCleanUpInterval = time.Minute * 15
)

var (
	ErrInvalidUserScannerStrategy = errors.New("invalid user scanner strategy")
	ErrInvalidMaxStalePeriod      = errors.New("max stale period must be positive")
	ErrInvalidCacheTTL            = errors.New("cache TTL must be >= 0")
	supportedStrategies           = []string{UserScanStrategyList, UserScanStrategyUserIndex}
)

func (c *UsersScannerConfig) Validate() error {
	if c.Strategy != UserScanStrategyList && c.Strategy != UserScanStrategyUserIndex {
		return ErrInvalidUserScannerStrategy
	}
	if c.Strategy == UserScanStrategyUserIndex && c.MaxStalePeriod <= 0 {
		return ErrInvalidMaxStalePeriod
	}
	if c.CacheTTL < 0 {
		return ErrInvalidCacheTTL
	}
	return nil
}

func (c *UsersScannerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.Strategy, prefix+"users-scanner.strategy", UserScanStrategyList, fmt.Sprintf("Strategy to use to scan users. Supported values are: %s.", strings.Join(supportedStrategies, ", ")))
	f.DurationVar(&c.MaxStalePeriod, prefix+"users-scanner.user-index.max-stale-period", time.Hour, "Maximum period of time to consider the user index as stale. Fall back to the base scanner if stale. Only valid when strategy is user_index.")
	f.DurationVar(&c.CleanUpInterval, prefix+"users-scanner.user-index.cleanup-interval", defaultCleanUpInterval, fmt.Sprintf("How frequently user index file is updated, it only take effect when user scan stratehy is %s.", UserScanStrategyUserIndex))
	f.DurationVar(&c.CacheTTL, prefix+"users-scanner.cache-ttl", 0, "TTL of the cached users. 0 disables caching and relies on caching at bucket client level.")
}
