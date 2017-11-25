package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Masterminds/squirrel"
	"github.com/go-kit/kit/log/level"
	_ "github.com/lib/pq"                         // Import the postgres sql driver
	_ "github.com/mattes/migrate/driver/postgres" // Import the postgres migrations driver
	"github.com/mattes/migrate/migrate"
	"github.com/weaveworks/cortex/pkg/configs"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	// TODO: These are a legacy from when configs was more general. Update the
	// schema so this isn't needed.
	entityType = "org"
	subsystem  = "cortex"
)

var (
	activeConfig = squirrel.Eq{
		"deleted_at": nil,
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

// New creates a new postgres DB
func New(uri, migrationsDir string) (DB, error) {
	if migrationsDir != "" {
		level.Info(util.Logger).Log("msg", "running database migrations...")
		if errs, ok := migrate.UpSync(uri, migrationsDir); !ok {
			for _, err := range errs {
				level.Error(util.Logger).Log("err", err)
			}
			return DB{}, errors.New("database migrations failed")
		}
	}
	db, err := sql.Open("postgres", uri)
	return DB{
		dbProxy:              db,
		StatementBuilderType: statementBuilder(db),
	}, err
}

var statementBuilder = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).RunWith

func (d DB) findConfigs(filter squirrel.Sqlizer) (map[string]configs.View, error) {
	rows, err := d.Select("id", "owner_id", "config").
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
		err = rows.Scan(&cfg.ID, &userID, &cfgBytes)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(cfgBytes, &cfg.Config)
		if err != nil {
			return nil, err
		}
		cfgs[userID] = cfg
	}
	return cfgs, nil
}

// GetConfig gets a configuration.
func (d DB) GetConfig(userID string) (configs.View, error) {
	var cfgView configs.View
	var cfgBytes []byte
	err := d.Select("id", "config").
		From("configs").
		Where(squirrel.And{activeConfig, squirrel.Eq{"owner_id": userID}}).
		OrderBy("id DESC").
		Limit(1).
		QueryRow().Scan(&cfgView.ID, &cfgBytes)
	if err != nil {
		return cfgView, err
	}
	err = json.Unmarshal(cfgBytes, &cfgView.Config)
	return cfgView, err
}

// SetConfig sets a configuration.
func (d DB) SetConfig(userID string, cfg configs.Config) error {
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
	return d.findConfigs(activeConfig)
}

// GetConfigs gets all of the configs that have changed recently.
func (d DB) GetConfigs(since configs.ID) (map[string]configs.View, error) {
	return d.findConfigs(squirrel.And{
		activeConfig,
		squirrel.Gt{"id": since},
	})
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
