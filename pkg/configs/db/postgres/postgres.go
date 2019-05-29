package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	migrate "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // Import the postgres migrations driver
	"github.com/lib/pq"
	_ "github.com/lib/pq" // Import the postgres sql driver
	"github.com/pkg/errors"
	_ "gopkg.in/golang-migrate/migrate.v1/driver/postgres" // Import the postgres migrations driver
	"gopkg.in/golang-migrate/migrate.v1/migrate"
)

const (
	// TODO: These are a legacy from when configs was more general. Update the
	// schema so this isn't needed.
	entityType = "org"
	subsystem  = "cortex"
	// timeout waiting for database connection to be established
	dbTimeout = 5 * time.Minute
)

var (
	allConfigs = squirrel.Eq{
		"owner_type": entityType,
		"subsystem":  subsystem,
	}
)

// DB is a postgres db, for dev and production
type DB struct {
	dbProxy
	squirrel.StatementBuilderType
}

type dbProxy interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Prepare(query string) (*sql.Stmt, error)
}

// dbWait waits for database connection to be established
func dbWait(db *sql.DB) error {
	deadline := time.Now().Add(dbTimeout)
	var err error
	for tries := 0; time.Now().Before(deadline); tries++ {
		err = db.Ping()
		if err == nil {
			return nil
		}
		level.Warn(util.Logger).Log("msg", "db connection not established, retrying...", "error", err)
		time.Sleep(time.Second << uint(tries))
	}
	return errors.Wrapf(err, "db connection not established after %s", dbTimeout)
}

// New creates a new postgres DB
func New(uri, migrationsDir string) (DB, error) {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return DB{}, errors.Wrap(err, "cannot open postgres db")
	}

	if err := dbWait(db); err != nil {
		return DB{}, errors.Wrap(err, "cannot establish db connection")
	}

	if migrationsDir != "" {
		level.Info(util.Logger).Log("msg", "running database migrations...")

		m, err := migrate.New(uri, migrationsDir)
		if err != nil {
			level.Error(util.Logger).Log("err", err)
		}

		migrationErr := m.Up()
		if migrationErr != nil {
			level.Error(util.Logger).Log("err", migrationErr)
			return DB{}, errors.New("database migrations failed")
		}
	}

	return DB{
		dbProxy:              db,
		StatementBuilderType: statementBuilder(db),
	}, err
}

var statementBuilder = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).RunWith

func (d DB) findConfigs(filter squirrel.Sqlizer) (map[string]configs.View, error) {
	rows, err := d.Select("id", "owner_id", "config", "deleted_at").
		Options("DISTINCT ON (owner_id)").
		From("configs").
		Where(filter).
		OrderBy("owner_id, id DESC").
		Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cfgs := map[string]configs.View{}
	for rows.Next() {
		var cfg configs.View
		var cfgBytes []byte
		var userID string
		var deletedAt pq.NullTime
		err = rows.Scan(&cfg.ID, &userID, &cfgBytes, &deletedAt)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(cfgBytes, &cfg.Config)
		if err != nil {
			return nil, err
		}
		cfg.DeletedAt = deletedAt.Time
		cfgs[userID] = cfg
	}
	return cfgs, nil
}

// GetConfig gets a configuration.
func (d DB) GetConfig(userID string) (configs.View, error) {
	var cfgView configs.View
	var cfgBytes []byte
	var deletedAt pq.NullTime
	err := d.Select("id", "config", "deleted_at").
		From("configs").
		Where(squirrel.And{allConfigs, squirrel.Eq{"owner_id": userID}}).
		OrderBy("id DESC").
		Limit(1).
		QueryRow().Scan(&cfgView.ID, &cfgBytes, &deletedAt)
	if err != nil {
		return cfgView, err
	}
	cfgView.DeletedAt = deletedAt.Time
	err = json.Unmarshal(cfgBytes, &cfgView.Config)
	return cfgView, err
}

// SetConfig sets a configuration.
func (d DB) SetConfig(userID string, cfg configs.Config) error {
	if !cfg.RulesConfig.FormatVersion.IsValid() {
		return fmt.Errorf("invalid rule format version %v", cfg.RulesConfig.FormatVersion)
	}
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	_, err = d.Insert("configs").
		Columns("owner_id", "owner_type", "subsystem", "config").
		Values(userID, entityType, subsystem, cfgBytes).
		Exec()
	return err
}

// GetAllConfigs gets all of the configs.
func (d DB) GetAllConfigs() (map[string]configs.View, error) {
	return d.findConfigs(allConfigs)
}

// GetConfigs gets all of the configs that have changed recently.
func (d DB) GetConfigs(since configs.ID) (map[string]configs.View, error) {
	return d.findConfigs(squirrel.And{
		allConfigs,
		squirrel.Gt{"id": since},
	})
}

// GetRulesConfig gets the latest alertmanager config for a user.
func (d DB) GetRulesConfig(userID string) (configs.VersionedRulesConfig, error) {
	current, err := d.GetConfig(userID)
	if err != nil {
		return configs.VersionedRulesConfig{}, err
	}
	cfg := current.GetVersionedRulesConfig()
	if cfg == nil {
		return configs.VersionedRulesConfig{}, sql.ErrNoRows
	}
	return *cfg, nil
}

// SetRulesConfig sets the current alertmanager config for a user.
func (d DB) SetRulesConfig(userID string, oldConfig, newConfig configs.RulesConfig) (bool, error) {
	updated := false
	err := d.Transaction(func(tx DB) error {
		current, err := d.GetConfig(userID)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		// The supplied oldConfig must match the current config. If no config
		// exists, then oldConfig must be nil. Otherwise, it must exactly
		// equal the existing config.
		if !((err == sql.ErrNoRows && oldConfig.Files == nil) || oldConfig.Equal(current.Config.RulesConfig)) {
			return nil
		}
		new := configs.Config{
			AlertmanagerConfig: current.Config.AlertmanagerConfig,
			RulesConfig:        newConfig,
		}
		updated = true
		return d.SetConfig(userID, new)
	})
	return updated, err
}

// findRulesConfigs helps GetAllRulesConfigs and GetRulesConfigs retrieve the
// set of all active rules configurations across all our users.
func (d DB) findRulesConfigs(filter squirrel.Sqlizer) (map[string]configs.VersionedRulesConfig, error) {
	rows, err := d.Select("id", "owner_id", "config ->> 'rules_files'", "config ->> 'rule_format_version'", "deleted_at").
		Options("DISTINCT ON (owner_id)").
		From("configs").
		Where(filter).
		// `->>` gets a JSON object field as text. When a config row exists
		// and alertmanager config is provided but ruler config has not yet
		// been, the 'rules_files' key will have an empty JSON object as its
		// value. This is (probably) the most efficient way to test for a
		// non-empty `rules_files` key.
		//
		// This whole situation is way too complicated. See
		// https://github.com/cortexproject/cortex/issues/619 for the whole
		// story, and our plans to improve it.
		Where("config ->> 'rules_files' <> '{}'").
		OrderBy("owner_id, id DESC").
		Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cfgs := map[string]configs.VersionedRulesConfig{}
	for rows.Next() {
		var cfg configs.VersionedRulesConfig
		var userID string
		var cfgBytes []byte
		var rfvBytes []byte
		var deletedAt pq.NullTime
		err = rows.Scan(&cfg.ID, &userID, &cfgBytes, &rfvBytes, &deletedAt)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(cfgBytes, &cfg.Config.Files)
		if err != nil {
			return nil, err
		}
		// Legacy configs don't have a rule format version, in which case this will
		// be a zero-length (but non-nil) slice.
		if len(rfvBytes) > 0 {
			err = json.Unmarshal([]byte(`"`+string(rfvBytes)+`"`), &cfg.Config.FormatVersion)
			if err != nil {
				return nil, err
			}
		}
		cfg.DeletedAt = deletedAt.Time
		cfgs[userID] = cfg
	}
	return cfgs, nil
}

// GetAllRulesConfigs gets all alertmanager configs for all users.
func (d DB) GetAllRulesConfigs() (map[string]configs.VersionedRulesConfig, error) {
	return d.findRulesConfigs(allConfigs)
}

// GetRulesConfigs gets all the alertmanager configs that have changed since a given config.
func (d DB) GetRulesConfigs(since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	return d.findRulesConfigs(squirrel.And{
		allConfigs,
		squirrel.Gt{"id": since},
	})
}

// SetDeletedAtConfig sets a deletedAt for configuration
// by adding a single new row with deleted_at set
// the same as SetConfig is actually insert
func (d DB) SetDeletedAtConfig(userID string, deletedAt pq.NullTime, cfg configs.Config) error {
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = d.Insert("configs").
		Columns("owner_id", "owner_type", "subsystem", "deleted_at", "config").
		Values(userID, entityType, subsystem, deletedAt, cfgBytes).
		Exec()
	return err
}

// DeactivateConfig deactivates a configuration.
func (d DB) DeactivateConfig(userID string) error {
	cfg, err := d.GetConfig(userID)
	if err != nil {
		return err
	}
	return d.SetDeletedAtConfig(userID, pq.NullTime{Time: time.Now(), Valid: true}, cfg.Config)
}

// RestoreConfig restores configuration.
func (d DB) RestoreConfig(userID string) error {
	cfg, err := d.GetConfig(userID)
	if err != nil {
		return err
	}
	return d.SetDeletedAtConfig(userID, pq.NullTime{}, cfg.Config)
}

// Transaction runs the given function in a postgres transaction. If fn returns
// an error the txn will be rolled back.
func (d DB) Transaction(f func(DB) error) error {
	if _, ok := d.dbProxy.(*sql.Tx); ok {
		// Already in a nested transaction
		return f(d)
	}

	tx, err := d.dbProxy.(*sql.DB).Begin()
	if err != nil {
		return err
	}
	err = f(DB{
		dbProxy:              tx,
		StatementBuilderType: statementBuilder(tx),
	})
	if err != nil {
		// Rollback error is ignored as we already have one in progress
		if err2 := tx.Rollback(); err2 != nil {
			level.Warn(util.Logger).Log("msg", "transaction rollback error (ignored)", "error", err2)
		}
		return err
	}
	return tx.Commit()
}

// Close finishes using the db
func (d DB) Close() error {
	if db, ok := d.dbProxy.(interface {
		Close() error
	}); ok {
		return db.Close()
	}
	return nil
}
