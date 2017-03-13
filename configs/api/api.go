package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/configs"
	"github.com/weaveworks/cortex/configs/db"
)

// API implements the configs api.
type API struct {
	db db.DB
	http.Handler
}

// New creates a new API
func New(database db.DB) *API {
	a := &API{db: database}
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
		// Deprecated public APIs
		{"get_config", "GET", "/api/configs/org/cortex", a.getConfig},
		{"set_config", "POST", "/api/configs/org/cortex", a.setConfig},
		// Dedicated APIs for updating rules config. In future, these *must*
		// be used.
		{"get_rules", "GET", "/api/prom/rules", a.getConfig},
		{"set_rules", "POST", "/api/prom/rules", a.setConfig},
		{"get_alert_config", "GET", "/api/prom/alerts", a.getConfig},
		{"set_alert_config", "POST", "/api/prom/alerts", a.setConfig},
		// Deprecated internal APIs.
		{"private_get_configs", "GET", "/private/api/configs/org/cortex", a.getConfigs},
		// Internal APIs.
		{"private_get_rules", "GET", "/private/api/prom/rules", a.getConfigs},
		{"private_get_alerts", "GET", "/private/api/prom/alerts", a.getConfigs},
	} {
		r.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
}

// getConfig returns the request configuration.
func (a *API) getConfig(w http.ResponseWriter, r *http.Request) {
	userID, _, err := user.ExtractFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	cfg, err := a.db.GetConfig(userID)
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
	userID, _, err := user.ExtractFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var cfg configs.Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		// XXX: Untested
		log.Errorf("Error decoding json body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.db.SetConfig(userID, cfg); err != nil {
		// XXX: Untested
		log.Errorf("Error storing config: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ConfigsView renders multiple configurations, mapping userID to ConfigView.
// Exposed only for tests.
type ConfigsView struct {
	Configs map[string]configs.ConfigView `json:"configs"`
}

func (a *API) getConfigs(w http.ResponseWriter, r *http.Request) {
	var cfgs map[string]configs.ConfigView
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
