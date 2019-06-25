package ruler

import (
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/prometheus/pkg/rulefmt"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"
)

var (
	ErrNoNamespace = errors.New("a namespace must be provided in the url")
	ErrNoGroupName = errors.New("a matching group name must be provided in the url")
)

// API is used to provided endpoints to directly interact with the ruler
type API struct {
	store configs.RuleStore
}

// NewAPI returns a ruler API
func NewAPI(store configs.RuleStore) *API {
	return &API{store}
}

// RegisterRoutes registers the configs API HTTP routes with the provided Router.
func (a *API) RegisterRoutes(r *mux.Router) {
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"list_rules", "GET", "/api/prom/rules", a.listRules},
		{"list_rules_namespace", "GET", "/api/prom/rules/{namespace}/", a.listRules},
		{"get_rulegroup", "GET", "/api/prom/rules/{namespace}/{groupName}", a.getRuleGroup},
		{"set_namespace", "POST", "/api/prom/rules/{namespace}/", a.setRuleNamespace},
		{"set_rulegroup", "POST", "/api/prom/rules/{namespace}/", a.setRuleNamespace},
		{"delete_rulegroup", "DELETE", "/api/prom/rules/{namespace}/{groupName}", a.deleteRuleGroup},
	} {
		r.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
}

func (a *API) listRules(w http.ResponseWriter, r *http.Request) {
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

	vars := mux.Vars(r)

	rgs, err := a.store.ListRuleGroups(r.Context(), configs.RuleStoreConditions{
		UserID:    userID,
		Namespace: vars["namespace"],
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	d, err := yaml.Marshal(&rgs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if err := yaml.NewEncoder(w).Encode(d); err != nil {
		level.Error(logger).Log("msg", "error marshalling yaml rule groups", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *API) getRuleGroup(w http.ResponseWriter, r *http.Request) {

}

func (a *API) setRuleNamespace(w http.ResponseWriter, r *http.Request) {
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

	vars := mux.Vars(r)

	namespace, set := vars["namespace"]
	if !set {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, ErrNoNamespace.Error(), http.StatusBadRequest)
		return
	}

	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rgs := []rulefmt.RuleGroup{}
	err = yaml.Unmarshal(payload, &rgs)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, rg := range rgs {
		a.store.SetRuleGroup(r.Context(), userID, namespace, rg)
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) setRuleGroup(w http.ResponseWriter, r *http.Request) {
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

	vars := mux.Vars(r)

	namespace, set := vars["namespace"]
	if !set {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, ErrNoNamespace.Error(), http.StatusBadRequest)
		return
	}

	groupName, set := vars["groupName"]
	if !set {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, ErrNoGroupName.Error(), http.StatusBadRequest)
		return
	}

	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rg := rulefmt.RuleGroup{}
	err = yaml.Unmarshal(payload, &rg)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Set the group name to match the name provided in
	// the url
	rg.Name = groupName
	errs := configs.ValidateRuleGroup(rg)
	if len(errs) > 0 {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, errs[0].Error(), http.StatusBadRequest)
		return
	}

	err = a.store.SetRuleGroup(r.Context(), userID, namespace, rg)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *API) deleteRuleGroup(w http.ResponseWriter, r *http.Request) {

}
