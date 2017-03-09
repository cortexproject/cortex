package api

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"

	"github.com/weaveworks/cortex/configs"
	"github.com/weaveworks/cortex/configs/db"
)

const (
	// DefaultOrgIDHeader is the default OrgID header.
	DefaultOrgIDHeader = "X-Scope-OrgID"
)

// API implements the configs api.
type API struct {
	db db.DB
	http.Handler
	Config
}

// Config describes the configuration for the configs API.
type Config struct {
	OrgIDHeader string
}

// RegisterFlags adds the flags required to configure this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.OrgIDHeader, "org-id-header", DefaultOrgIDHeader, "Name of header that contains org ID")
}

// New creates a new API
func New(config Config, database db.DB) *API {
	a := &API{Config: config, db: database}
	r := mux.NewRouter()
	a.RegisterRoutes(r)
	a.Handler = r
	return a
}

func (a *API) admin(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "text/html")
	fmt.Fprintf(w, `
<!doctype html>
<html>
	<head><title>configs :: configuration service</title></head>
	<body>
		<h1>configs :: configuration service</h1>
	</body>
</html>
`)
}

// RegisterRoutes registers the configs API HTTP routes with the provided Router.
func (a *API) RegisterRoutes(r *mux.Router) {
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"root", "GET", "/", a.admin},
		{"get_config", "GET", "/api/configs/org/cortex", a.getConfig},
		{"set_config", "POST", "/api/configs/org/cortex", a.setConfig},
		// Internal APIs.
		{"private_get_configs", "GET", "/private/api/configs/org/cortex", a.getConfigs},
	} {
		r.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
}

// authorize checks whether the given header provides access to entity.
func authorize(r *http.Request, header, entityID string) (string, int) {
	token := r.Header.Get(header)
	if token == "" {
		return "", http.StatusUnauthorized
	}
	return token, 0
}

func (a *API) authorizeOrg(r *http.Request) (configs.OrgID, int) {
	entity, err := authorize(r, a.OrgIDHeader, "orgID")
	return configs.OrgID(entity), err
}

// getConfig returns the request configuration.
func (a *API) getConfig(w http.ResponseWriter, r *http.Request) {
	orgID, code := a.authorizeOrg(r)
	if code != 0 {
		w.WriteHeader(code)
		return
	}
	cfg, err := a.db.GetConfig(orgID)
	if err == sql.ErrNoRows {
		http.Error(w, "No configuration", http.StatusNotFound)
		return
	} else if err != nil {
		// XXX: Untested
		log.Errorf("Error getting config: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(cfg); err != nil {
		// XXX: Untested
		log.Errorf("Error encoding config: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *API) setConfig(w http.ResponseWriter, r *http.Request) {
	orgID, code := a.authorizeOrg(r)
	if code != 0 {
		w.WriteHeader(code)
		return
	}
	var cfg configs.Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		// XXX: Untested
		log.Errorf("Error decoding json body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.db.SetConfig(orgID, cfg); err != nil {
		// XXX: Untested
		log.Errorf("Error storing config: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ConfigsView renders multiple configurations.
// Exposed only for tests.
type ConfigsView struct {
	Configs map[configs.OrgID]configs.ConfigView `json:"configs"`
}

func (a *API) getConfigs(w http.ResponseWriter, r *http.Request) {
	var cfgs map[configs.OrgID]configs.ConfigView
	var err error
	rawSince := r.FormValue("since")
	if rawSince == "" {
		cfgs, err = a.db.GetAllConfigs()
	} else {
		since, err := strconv.ParseUint(rawSince, 10, 0)
		if err != nil {
			log.Infof("Invalid config ID: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cfgs, err = a.db.GetConfigs(configs.ID(since))
	}

	if err != nil {
		// XXX: Untested
		log.Errorf("Error getting configs: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	view := ConfigsView{Configs: cfgs}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(view); err != nil {
		// XXX: Untested
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
