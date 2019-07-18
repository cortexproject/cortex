package alertmanager

import (
	"io/ioutil"
	"net/http"

	"github.com/cortexproject/cortex/pkg/alertmanager/storage"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"
)

// RegisterRoutes registers the configs API HTTP routes with the provided Router.
func (am *MultitenantAlertmanager) RegisterRoutes(r *mux.Router) {
	// if no storeis set return without regisering routes
	if am.store == nil {
		return
	}
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"get_config", "GET", "/api/prom/alertmanager", am.getUserConfig},
		{"set_config", "POST", "/api/prom/alertmanager", am.setUserConfig},
		{"delete_config", "DELETE", "/api/prom/alertmanager", am.deleteUserConfig},
	} {
		r.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
}

func (am *MultitenantAlertmanager) getUserConfig(w http.ResponseWriter, r *http.Request) {
	userID, _, err := user.ExtractOrgIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	logger := util.WithContext(r.Context(), util.Logger)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if userID == "" {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	cfg, err := am.store.GetAlertConfig(r.Context(), userID)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	d, err := yaml.Marshal(&cfg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if _, err := w.Write(d); err != nil {
		level.Error(logger).Log("msg", "error marshalling yaml alertmanager config", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (am *MultitenantAlertmanager) setUserConfig(w http.ResponseWriter, r *http.Request) {
	userID, _, err := user.ExtractOrgIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	logger := util.WithContext(r.Context(), util.Logger)

	if userID == "" {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cfg := storage.AlertConfig{}
	err = yaml.Unmarshal(payload, &cfg)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = am.store.SetAlertConfig(r.Context(), userID, cfg)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (am *MultitenantAlertmanager) deleteUserConfig(w http.ResponseWriter, r *http.Request) {

}
