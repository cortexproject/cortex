package ruler

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/configs"
	"github.com/weaveworks/cortex/pkg/configs/db"
	"github.com/weaveworks/cortex/pkg/util"
)

// API implements the configs api.
type API struct {
	db db.RulesDB
	http.Handler
}

// NewAPIFromConfig makes a new API from our database config.
func NewAPIFromConfig(cfg db.Config) (*API, error) {
	db, err := db.NewRulesDB(cfg)
	if err != nil {
		return nil, err
	}
	return NewAPI(db), nil
}

// NewAPI creates a new API.
func NewAPI(db db.RulesDB) *API {
	a := &API{db: db}
	r := mux.NewRouter()
	a.RegisterRoutes(r)
	a.Handler = r
	return a
}

// RegisterRoutes registers the configs API HTTP routes with the provided Router.
func (a *API) RegisterRoutes(r *mux.Router) {
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"get_rules", "GET", "/api/prom/rules", a.getConfig},
		{"cas_rules", "POST", "/api/prom/rules", a.casConfig},
	} {
		r.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
}

// getConfig returns the request configuration.
func (a *API) getConfig(w http.ResponseWriter, r *http.Request) {
	userID, _, err := user.ExtractOrgIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	logger := util.WithContext(r.Context(), util.Logger)

	cfg, err := a.db.GetRulesConfig(userID)
	if err == sql.ErrNoRows {
		http.Error(w, "No configuration", http.StatusNotFound)
		return
	} else if err != nil {
		level.Error(logger).Log("msg", "error getting config", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(cfg); err != nil {
		level.Error(logger).Log("msg", "error encoding config", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type configUpdateRequest struct {
	OldConfig configs.RulesConfig `json:"old_config"`
	NewConfig configs.RulesConfig `json:"new_config"`
}

func (a *API) casConfig(w http.ResponseWriter, r *http.Request) {
	userID, _, err := user.ExtractOrgIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	logger := util.WithContext(r.Context(), util.Logger)

	var updateReq configUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&updateReq); err != nil {
		level.Error(logger).Log("msg", "error decoding json body", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if _, err = updateReq.NewConfig.Parse(); err != nil {
		level.Error(logger).Log("msg", "invalid rules", "err", err)
		http.Error(w, fmt.Sprintf("Invalid rules: %v", err), http.StatusBadRequest)
		return
	}

	updated, err := a.db.SetRulesConfig(userID, updateReq.OldConfig, updateReq.NewConfig)
	if err != nil {
		level.Error(logger).Log("msg", "error storing config", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !updated {
		http.Error(w, "Supplied configuration doesn't match current configuration", http.StatusConflict)
	}
	w.WriteHeader(http.StatusNoContent)
}
