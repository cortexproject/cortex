package ruler

import (
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/prometheus/prometheus/pkg/rulefmt"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v2"
)

var (
	ErrNoNamespace  = errors.New("a namespace must be provided in the url")
	ErrNoGroupName  = errors.New("a matching group name must be provided in the url")
	ErrNoRuleGroups = errors.New("no rule groups found")
)

// API is used to provided endpoints to directly interact with the ruler
type API struct {
	store RuleStore
}

// NewAPI returns a ruler API
func NewAPI(store RuleStore) *API {
	return &API{store}
}

// RegisterRoutes registers the configs API HTTP routes with the provided Router.
func (a *API) RegisterRoutes(r *mux.Router) {
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"list_rules", "GET", "/api/prom/rules", a.listRules},
		{"list_rules_namespace", "GET", "/api/prom/rules/{namespace}", a.listRules},
		{"get_rulegroup", "GET", "/api/prom/rules/{namespace}/{groupName}", a.getRuleGroup},
		{"set_rulegroup", "POST", "/api/prom/rules/{namespace}", a.createRuleGroup},
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

	options := RuleStoreConditions{
		UserID: userID,
	}

	vars := mux.Vars(r)

	namespace := vars["namespace"]
	if namespace != "" {
		level.Debug(logger).Log("msg", "retrieving rule groups with namespace", "userID", userID, "namespace", namespace)
		options.Namespace = namespace
	}

	level.Debug(logger).Log("msg", "retrieving rule groups from rule store", "userID", userID)
	rgs, err := a.store.ListRuleGroups(r.Context(), options)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(logger).Log("msg", "retrieved rule groups from rule store", "userID", userID, "num_namespaces", len(rgs))

	if len(rgs) == 0 {
		level.Info(logger).Log("msg", "no rule groups found", "userID", userID)
		http.Error(w, ErrNoRuleGroups.Error(), http.StatusNotFound)
		return
	}

	d, err := yaml.Marshal(&rgs)
	if err != nil {
		level.Error(logger).Log("msg", "error marshalling yaml rule groups", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if _, err := w.Write(d); err != nil {
		level.Error(logger).Log("msg", "error writing yaml response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *API) getRuleGroup(w http.ResponseWriter, r *http.Request) {
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
	ns, exists := vars["namespace"]
	if !exists {
		http.Error(w, ErrNoNamespace.Error(), http.StatusUnauthorized)
		return
	}

	gn, exists := vars["groupName"]
	if !exists {
		http.Error(w, ErrNoGroupName.Error(), http.StatusUnauthorized)
		return
	}

	rg, err := a.store.GetRuleGroup(r.Context(), userID, ns, gn)
	if err != nil {
		if err == ErrGroupNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	d, err := yaml.Marshal(&rg)
	if err != nil {
		level.Error(logger).Log("msg", "error marshalling yaml rule groups", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if _, err := w.Write(d); err != nil {
		level.Error(logger).Log("msg", "error writing yaml response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *API) createRuleGroup(w http.ResponseWriter, r *http.Request) {
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

	namespace := vars["namespace"]
	if namespace == "" {
		level.Error(logger).Log("err", "no namespace provided with rule group")
		http.Error(w, ErrNoNamespace.Error(), http.StatusBadRequest)
		return
	}

	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(logger).Log("msg", "attempting to unmarshal rulegroup", "userID", userID, "group", string(payload))

	rg := rulefmt.RuleGroup{}
	err = yaml.Unmarshal(payload, &rg)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	errs := ValidateRuleGroup(rg)
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

	// Return a status accepted because the rule has been stored and queued for polling, but is not currently active
	w.WriteHeader(http.StatusAccepted)
}

func (a *API) deleteRuleGroup(w http.ResponseWriter, r *http.Request) {

}
