package tsdb

import (
	"flag"
	"time"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/cacheutil"

	"github.com/cortexproject/cortex/pkg/util/tls"
)

type RedisClientConfig struct {
	Addresses  string `yaml:"addresses"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	DB         int    `yaml:"db"`
	MasterName string `yaml:"master_name"`

	PoolSize               int `yaml:"pool_size"`
	MinIdleConns           int `yaml:"min_idle_conns"`
	MaxGetMultiConcurrency int `yaml:"max_get_multi_concurrency"`
	GetMultiBatchSize      int `yaml:"get_multi_batch_size"`
	MaxSetMultiConcurrency int `yaml:"max_set_multi_concurrency"`
	SetMultiBatchSize      int `yaml:"set_multi_batch_size"`

	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
	MaxConnAge   time.Duration `yaml:"max_conn_age"`

	TLSEnabled bool             `yaml:"tls_enabled"`
	TLS        tls.ClientConfig `yaml:",inline"`

	// If not zero then client-side caching is enabled.
	// Client-side caching is when data is stored in memory
	// instead of fetching data each time.
	// See https://redis.io/docs/manual/client-side-caching/ for info.
	CacheSize int `yaml:"cache_size"`
}

func (cfg *RedisClientConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Addresses, prefix+"addresses", "", "Comma separated list of redis addresses. Supported prefixes are: dns+ (looked up as an A/AAAA query), dnssrv+ (looked up as a SRV query, dnssrvnoa+ (looked up as a SRV query, with no A/AAAA lookup made after that).")
	f.StringVar(&cfg.Username, prefix+"username", "", "Redis username.")
	f.StringVar(&cfg.Password, prefix+"password", "", "Redis password.")
	f.IntVar(&cfg.DB, prefix+"db", 0, "Database to be selected after connecting to the server.")
	f.DurationVar(&cfg.DialTimeout, prefix+"dial-timeout", time.Second*5, "Client dial timeout.")
	f.DurationVar(&cfg.ReadTimeout, prefix+"read-timeout", time.Second*3, "Client read timeout.")
	f.DurationVar(&cfg.WriteTimeout, prefix+"write-timeout", time.Second*3, "Client write timeout.")
	f.DurationVar(&cfg.IdleTimeout, prefix+"idle-timeout", time.Minute*5, "Amount of time after which client closes idle connections. Should be less than server's timeout. -1 disables idle timeout check.")
	f.DurationVar(&cfg.MaxConnAge, prefix+"max-conn-age", 0, "Connection age at which client retires (closes) the connection. Default 0 is to not close aged connections.")
	f.IntVar(&cfg.PoolSize, prefix+"pool-size", 100, "Maximum number of socket connections.")
	f.IntVar(&cfg.MinIdleConns, prefix+"min-idle-conns", 10, "Specifies the minimum number of idle connections, which is useful when it is slow to establish new connections.")
	f.IntVar(&cfg.MaxGetMultiConcurrency, prefix+"max-get-multi-concurrency", 100, "The maximum number of concurrent GetMulti() operations. If set to 0, concurrency is unlimited.")
	f.IntVar(&cfg.GetMultiBatchSize, prefix+"get-multi-batch-size", 100, "The maximum size per batch for mget.")
	f.IntVar(&cfg.MaxSetMultiConcurrency, prefix+"max-set-multi-concurrency", 100, "The maximum number of concurrent SetMulti() operations. If set to 0, concurrency is unlimited.")
	f.IntVar(&cfg.SetMultiBatchSize, prefix+"set-multi-batch-size", 100, "The maximum size per batch for pipeline set.")
	f.StringVar(&cfg.MasterName, prefix+"master-name", "", "Specifies the master's name. Must be not empty for Redis Sentinel.")
	f.IntVar(&cfg.CacheSize, prefix+"cache-size", 0, "If not zero then client-side caching is enabled. Client-side caching is when data is stored in memory instead of fetching data each time. See https://redis.io/docs/manual/client-side-caching/ for more info.")
	f.BoolVar(&cfg.TLSEnabled, prefix+"tls-enabled", false, "Whether to enable tls for redis connection.")
	cfg.TLS.RegisterFlagsWithPrefix(prefix, f)
}

// Validate the config.
func (cfg *RedisClientConfig) Validate() error {
	if cfg.Addresses == "" {
		return errNoIndexCacheAddresses
	}

	if cfg.TLSEnabled {
		if (cfg.TLS.CertPath != "") != (cfg.TLS.KeyPath != "") {
			return errors.New("both client key and certificate must be provided")
		}
	}

	return nil
}

func (cfg *RedisClientConfig) ToRedisClientConfig() cacheutil.RedisClientConfig {
	return cacheutil.RedisClientConfig{
		Addr:                   cfg.Addresses,
		Username:               cfg.Username,
		Password:               cfg.Password,
		DB:                     cfg.DB,
		MasterName:             cfg.MasterName,
		DialTimeout:            cfg.DialTimeout,
		ReadTimeout:            cfg.ReadTimeout,
		WriteTimeout:           cfg.WriteTimeout,
		PoolSize:               cfg.PoolSize,
		MinIdleConns:           cfg.MinIdleConns,
		IdleTimeout:            cfg.IdleTimeout,
		MaxConnAge:             cfg.MaxConnAge,
		MaxGetMultiConcurrency: cfg.MaxGetMultiConcurrency,
		GetMultiBatchSize:      cfg.GetMultiBatchSize,
		MaxSetMultiConcurrency: cfg.MaxSetMultiConcurrency,
		SetMultiBatchSize:      cfg.SetMultiBatchSize,
		TLSEnabled:             cfg.TLSEnabled,
		TLSConfig: cacheutil.TLSConfig{
			CAFile:             cfg.TLS.CAPath,
			KeyFile:            cfg.TLS.KeyPath,
			CertFile:           cfg.TLS.CertPath,
			ServerName:         cfg.TLS.ServerName,
			InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
		},
	}
}
