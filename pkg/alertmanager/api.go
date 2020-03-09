package alertmanager

import (
	"io/ioutil"
	"net/http"

	"github.com/cortexproject/cortex/pkg/alertmanager/alerts"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"
)

// UserConfig is used to communicate a users alertmanager configs
type UserConfig struct {
	TemplateFiles      map[string]string `yaml:"template_files"`
	AlertmanagerConfig string            `yaml:"alertmanager_config"`
}

// RegisterRoutes registers the configs API HTTP routes with the provided Router.
func (am *MultitenantAlertmanager) RegisterRoutes(r *mux.Router) {
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"get_config", "GET", "/alerts", am.getUserConfig},
		{"set_config", "POST", "/alerts", am.setUserConfig},
		{"delete_config", "DELETE", "/alerts", am.deleteUserConfig},
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

	d, err := yaml.Marshal(&UserConfig{
		TemplateFiles:      alerts.ParseTemplates(cfg),
		AlertmanagerConfig: cfg.RawConfig,
	})

	if err != nil {
		level.Error(logger).Log("msg", "error marshalling yaml alertmanager config", "err", err, "user", userID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if _, err := w.Write(d); err != nil {
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

	cfg := &UserConfig{}
	err = yaml.Unmarshal(payload, cfg)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cfgDesc, err := alerts.ToProto(cfg.AlertmanagerConfig, cfg.TemplateFiles, userID)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = am.store.SetAlertConfig(r.Context(), cfgDesc)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (am *MultitenantAlertmanager) deleteUserConfig(w http.ResponseWriter, r *http.Request) {
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

	am.store.DeleteAlertConfig(r.Context(), userID)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}
