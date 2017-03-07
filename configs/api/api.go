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
	// DefaultUserIDHeader is the default UserID header.
	DefaultUserIDHeader = "X-Scope-UserID"
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
	UserIDHeader string
	OrgIDHeader  string
}

// RegisterFlags adds the flags required to configure this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.UserIDHeader, "user-id-header", DefaultUserIDHeader, "Name of header that contains user ID")
	flag.StringVar(&cfg.OrgIDHeader, "org-id-header", DefaultOrgIDHeader, "Name of header that contains user ID")
}

// New creates a new API
func New(config Config, database db.DB) *API {
	a := &API{Config: config, db: database}
	a.Handler = a.routes()
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

func (a *API) routes() http.Handler {
	r := mux.NewRouter()
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"root", "GET", "/", a.admin},
		{"get_user_config", "GET", "/api/configs/user/{subsystem}", a.getUserConfig},
		{"set_user_config", "POST", "/api/configs/user/{subsystem}", a.setUserConfig},
		{"get_org_config", "GET", "/api/configs/org/{subsystem}", a.getOrgConfig},
		{"set_org_config", "POST", "/api/configs/org/{subsystem}", a.setOrgConfig},
		// Internal APIs.
		{"private_get_user_configs", "GET", "/private/api/configs/user/{subsystem}", a.getUserConfigs},
		{"private_get_org_configs", "GET", "/private/api/configs/org/{subsystem}", a.getOrgConfigs},
	} {
		r.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
	return r
}

// authorize checks whether the given header provides access to entity.
func authorize(r *http.Request, header, entityID string) (string, int) {
	token := r.Header.Get(header)
	if token == "" {
		return "", http.StatusUnauthorized
	}
	return token, 0
}

// authorizeUser checks whether the user in the headers matches the userID in
// the URL.
func (a *API) authorizeUser(r *http.Request) (configs.UserID, int) {
	entity, err := authorize(r, a.UserIDHeader, "userID")
	return configs.UserID(entity), err
}

// authorizeOrg checks whether the user in the headers matches the userID in
// the URL.
func (a *API) authorizeOrg(r *http.Request) (configs.OrgID, int) {
	entity, err := authorize(r, a.OrgIDHeader, "orgID")
	return configs.OrgID(entity), err
}

// getUserConfig returns the requested configuration.
func (a *API) getUserConfig(w http.ResponseWriter, r *http.Request) {
	userID, code := a.authorizeUser(r)
	if code != 0 {
		w.WriteHeader(code)
		return
	}

	vars := mux.Vars(r)
	subsystem := configs.Subsystem(vars["subsystem"])

	cfg, err := a.db.GetUserConfig(userID, subsystem)
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

func (a *API) setUserConfig(w http.ResponseWriter, r *http.Request) {
	userID, code := a.authorizeUser(r)
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

	vars := mux.Vars(r)
	subsystem := configs.Subsystem(vars["subsystem"])
	if err := a.db.SetUserConfig(userID, subsystem, cfg); err != nil {
		// XXX: Untested
		log.Errorf("Error storing config: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// getOrgConfig returns the request configuration.
func (a *API) getOrgConfig(w http.ResponseWriter, r *http.Request) {
	orgID, code := a.authorizeOrg(r)
	if code != 0 {
		w.WriteHeader(code)
		return
	}
	vars := mux.Vars(r)
	subsystem := configs.Subsystem(vars["subsystem"])

	cfg, err := a.db.GetOrgConfig(orgID, subsystem)
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

func (a *API) setOrgConfig(w http.ResponseWriter, r *http.Request) {
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

	vars := mux.Vars(r)
	subsystem := configs.Subsystem(vars["subsystem"])
	if err := a.db.SetOrgConfig(orgID, subsystem, cfg); err != nil {
		// XXX: Untested
		log.Errorf("Error storing config: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// OrgConfigsView renders multiple configurations.
// Exposed only for tests.
type OrgConfigsView struct {
	Configs map[configs.OrgID]configs.ConfigView `json:"configs"`
}

func (a *API) getOrgConfigs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	subsystem := configs.Subsystem(vars["subsystem"])

	var cfgs map[configs.OrgID]configs.ConfigView
	var err error
	rawSince := r.FormValue("since")
	if rawSince == "" {
		cfgs, err = a.db.GetAllOrgConfigs(subsystem)
	} else {
		since, err := strconv.ParseUint(rawSince, 10, 0)
		if err != nil {
			log.Infof("Invalid config ID: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cfgs, err = a.db.GetOrgConfigs(subsystem, configs.ID(since))
	}

	if err != nil {
		// XXX: Untested
		log.Errorf("Error getting configs: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	view := OrgConfigsView{Configs: cfgs}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(view); err != nil {
		// XXX: Untested
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// UserConfigsView renders multiple configurations.
// Exposed only for tests.
type UserConfigsView struct {
	Configs map[configs.UserID]configs.ConfigView `json:"configs"`
}

func (a *API) getUserConfigs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	subsystem := configs.Subsystem(vars["subsystem"])

	var cfgs map[configs.UserID]configs.ConfigView
	var err error
	rawSince := r.FormValue("since")
	if rawSince == "" {
		cfgs, err = a.db.GetAllUserConfigs(subsystem)
	} else {
		since, err := strconv.ParseUint(rawSince, 10, 0)
		if err != nil {
			log.Infof("Invalid config ID: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cfgs, err = a.db.GetUserConfigs(subsystem, configs.ID(since))
	}

	if err != nil {
		// XXX: Untested
		log.Errorf("Error getting configs: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	view := UserConfigsView{Configs: cfgs}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(view); err != nil {
		// XXX: Untested
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
