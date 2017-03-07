package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Masterminds/squirrel"
	"github.com/Sirupsen/logrus"
	_ "github.com/lib/pq"                         // Import the postgres sql driver
	_ "github.com/mattes/migrate/driver/postgres" // Import the postgres migrations driver
	"github.com/mattes/migrate/migrate"
	"github.com/weaveworks/cortex/configs"
)

const (
	orgType  = "org"
	userType = "user"
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
func New(databaseURI, migrationsDir string) (DB, error) {
	if migrationsDir != "" {
		logrus.Infof("Running Database Migrations...")
		if errs, ok := migrate.UpSync(databaseURI, migrationsDir); !ok {
			for _, err := range errs {
				logrus.Error(err)
			}
			return DB{}, errors.New("Database migrations failed")
		}
	}
	db, err := sql.Open("postgres", databaseURI)
	return DB{
		dbProxy:              db,
		StatementBuilderType: statementBuilder(db),
	}, err
}

var statementBuilder = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).RunWith

func configMatches(id, entityType, subsystem string) squirrel.Sqlizer {
	// TODO: Tests for deleted_at requirement.
	return squirrel.And{
		configsMatch(entityType, subsystem),
		squirrel.Eq{
			"owner_id": id,
		},
	}
}

// configsMatch returns a matcher for configs of a particular type.
func configsMatch(entityType, subsystem string) squirrel.Sqlizer {
	return squirrel.Eq{
		"deleted_at": nil,
		"owner_type": entityType,
		"subsystem":  subsystem,
	}
}

func (d DB) findConfig(entityID, entityType, subsystem string) (configs.ConfigView, error) {
	var cfgView configs.ConfigView
	var cfgBytes []byte
	err := d.Select("id", "config").
		From("configs").
		Where(configMatches(entityID, entityType, subsystem)).
		OrderBy("id DESC").
		Limit(1).
		QueryRow().Scan(&cfgView.ID, &cfgBytes)
	if err != nil {
		return cfgView, err
	}
	err = json.Unmarshal(cfgBytes, &cfgView.Config)
	return cfgView, err
}

func (d DB) findConfigs(filter squirrel.Sqlizer) (map[string]configs.ConfigView, error) {
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
	cfgs := map[string]configs.ConfigView{}
	for rows.Next() {
		var cfg configs.ConfigView
		var cfgBytes []byte
		var entityID string
		err = rows.Scan(&cfg.ID, &entityID, &cfgBytes)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(cfgBytes, &cfg.Config)
		if err != nil {
			return nil, err
		}
		cfgs[entityID] = cfg
	}
	return cfgs, nil
}

func (d DB) insertConfig(id, entityType string, subsystem configs.Subsystem, cfg configs.Config) error {
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = d.Insert("configs").
		Columns("owner_id", "owner_type", "subsystem", "config").
		Values(id, entityType, string(subsystem), cfgBytes).
		Exec()
	return err
}

// GetUserConfig gets a user's configuration.
func (d DB) GetUserConfig(userID configs.UserID, subsystem configs.Subsystem) (configs.ConfigView, error) {
	return d.findConfig(string(userID), userType, string(subsystem))
}

// SetUserConfig sets a user's configuration.
func (d DB) SetUserConfig(userID configs.UserID, subsystem configs.Subsystem, cfg configs.Config) error {
	return d.insertConfig(string(userID), userType, subsystem, cfg)
}

// GetOrgConfig gets a org's configuration.
func (d DB) GetOrgConfig(orgID configs.OrgID, subsystem configs.Subsystem) (configs.ConfigView, error) {
	return d.findConfig(string(orgID), orgType, string(subsystem))
}

// SetOrgConfig sets a org's configuration.
func (d DB) SetOrgConfig(orgID configs.OrgID, subsystem configs.Subsystem, cfg configs.Config) error {
	return d.insertConfig(string(orgID), orgType, subsystem, cfg)
}

// toOrgConfigs = mapKeys configs.OrgID
func toOrgConfigs(rawCfgs map[string]configs.ConfigView) map[configs.OrgID]configs.ConfigView {
	cfgs := map[configs.OrgID]configs.ConfigView{}
	for entityID, cfg := range rawCfgs {
		cfgs[configs.OrgID(entityID)] = cfg
	}
	return cfgs
}

// GetAllOrgConfigs gets all of the organization configs for a subsystem.
func (d DB) GetAllOrgConfigs(subsystem configs.Subsystem) (map[configs.OrgID]configs.ConfigView, error) {
	rawCfgs, err := d.findConfigs(configsMatch(orgType, string(subsystem)))
	if err != nil {
		return nil, err
	}
	return toOrgConfigs(rawCfgs), nil
}

// GetOrgConfigs gets all of the organization configs for a subsystem that
// have changed recently.
func (d DB) GetOrgConfigs(subsystem configs.Subsystem, since configs.ID) (map[configs.OrgID]configs.ConfigView, error) {
	rawCfgs, err := d.findConfigs(squirrel.And{
		configsMatch(orgType, string(subsystem)),
		squirrel.Gt{"id": since},
	})
	if err != nil {
		return nil, err
	}
	return toOrgConfigs(rawCfgs), nil
}

// toUserConfigs = mapKeys configs.UserID
func toUserConfigs(rawCfgs map[string]configs.ConfigView) map[configs.UserID]configs.ConfigView {
	cfgs := map[configs.UserID]configs.ConfigView{}
	for entityID, cfg := range rawCfgs {
		cfgs[configs.UserID(entityID)] = cfg
	}
	return cfgs
}

// GetAllUserConfigs gets all of the user configs for a subsystem.
func (d DB) GetAllUserConfigs(subsystem configs.Subsystem) (map[configs.UserID]configs.ConfigView, error) {
	rawCfgs, err := d.findConfigs(configsMatch(userType, string(subsystem)))
	if err != nil {
		return nil, err
	}
	return toUserConfigs(rawCfgs), nil
}

// GetUserConfigs gets all of the user configs for a subsystem that have
// changed recently.
func (d DB) GetUserConfigs(subsystem configs.Subsystem, since configs.ID) (map[configs.UserID]configs.ConfigView, error) {
	rawCfgs, err := d.findConfigs(squirrel.And{
		configsMatch(userType, string(subsystem)),
		squirrel.Gt{"id": since},
	})
	if err != nil {
		return nil, err
	}
	return toUserConfigs(rawCfgs), nil
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
			logrus.Warn("transaction rollback: %v (ignored)", err2)
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
