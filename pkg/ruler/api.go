package ruler

import (
	"encoding/json"
	"fmt"
	io "io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
	"github.com/cortexproject/cortex/pkg/ruler/rulestore"
	"github.com/cortexproject/cortex/pkg/tenant"
	util_api "github.com/cortexproject/cortex/pkg/util/api"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

// In order to reimplement the prometheus rules API, a large amount of code was copied over
// This is required because the prometheus api implementation does not allow us to return errors
// on rule lookups, which might fail in Cortex's case.

// AlertDiscovery has info for all active alerts.
type AlertDiscovery struct {
	Alerts []*Alert `json:"alerts"`
}

// Alert has info for an alert.
type Alert struct {
	Labels          labels.Labels `json:"labels"`
	Annotations     labels.Labels `json:"annotations"`
	State           string        `json:"state"`
	ActiveAt        *time.Time    `json:"activeAt"`
	KeepFiringSince *time.Time    `json:"keepFiringSince,omitempty"`
	Value           string        `json:"value"`
}

// RuleDiscovery has info for all rules
type RuleDiscovery struct {
	RuleGroups []*RuleGroup `json:"groups"`
}

// RuleGroup has info for rules which are part of a group
type RuleGroup struct {
	Name string `json:"name"`
	File string `json:"file"`
	// In order to preserve rule ordering, while exposing type (alerting or recording)
	// specific properties, both alerting and recording rules are exposed in the
	// same array.
	Rules          []rule    `json:"rules"`
	Interval       float64   `json:"interval"`
	LastEvaluation time.Time `json:"lastEvaluation"`
	EvaluationTime float64   `json:"evaluationTime"`
	Limit          int64     `json:"limit"`
}

type rule interface{}

type alertingRule struct {
	// State can be "pending", "firing", "inactive".
	State          string        `json:"state"`
	Name           string        `json:"name"`
	Query          string        `json:"query"`
	Duration       float64       `json:"duration"`
	KeepFiringFor  float64       `json:"keepFiringFor"`
	Labels         labels.Labels `json:"labels"`
	Annotations    labels.Labels `json:"annotations"`
	Alerts         []*Alert      `json:"alerts"`
	Health         string        `json:"health"`
	LastError      string        `json:"lastError"`
	Type           v1.RuleType   `json:"type"`
	LastEvaluation time.Time     `json:"lastEvaluation"`
	EvaluationTime float64       `json:"evaluationTime"`
}

type recordingRule struct {
	Name           string        `json:"name"`
	Query          string        `json:"query"`
	Labels         labels.Labels `json:"labels"`
	Health         string        `json:"health"`
	LastError      string        `json:"lastError"`
	Type           v1.RuleType   `json:"type"`
	LastEvaluation time.Time     `json:"lastEvaluation"`
	EvaluationTime float64       `json:"evaluationTime"`
}

// API is used to handle HTTP requests for the ruler service
type API struct {
	ruler *Ruler
	store rulestore.RuleStore

	logger log.Logger
}

// NewAPI returns a new API struct with the provided ruler and rule store
func NewAPI(r *Ruler, s rulestore.RuleStore, logger log.Logger) *API {
	return &API{
		ruler:  r,
		store:  s,
		logger: logger,
	}
}

func (a *API) PrometheusRules(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), a.logger)
	userID, err := tenant.TenantID(req.Context())
	if err != nil || userID == "" {
		level.Error(logger).Log("msg", "error extracting org id from context", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, "no valid org id found", http.StatusBadRequest)
		return
	}

	if err := req.ParseForm(); err != nil {
		level.Error(logger).Log("msg", "error parsing form/query params", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, "error parsing form/query params", http.StatusBadRequest)
		return
	}

	typ := strings.ToLower(req.URL.Query().Get("type"))
	if typ != "" && typ != alertingRuleFilter && typ != recordingRuleFilter {
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("unsupported rule type %q", typ), http.StatusBadRequest)
		return
	}

	state := strings.ToLower(req.URL.Query().Get("state"))
	if state != "" && state != firingStateFilter && state != pendingStateFilter && state != inactiveStateFilter {
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("unsupported state value %q", state), http.StatusBadRequest)
		return
	}

	health := strings.ToLower(req.URL.Query().Get("health"))
	if health != "" && health != unknownHealthFilter && health != okHealthFilter && health != errHealthFilter {
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("unsupported health value %q", health), http.StatusBadRequest)
		return
	}

	_, err = parseMatchersParam(req.Form["match[]"])
	if err != nil {
		level.Error(logger).Log("msg", "error parsing match query params", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("error parsing match params %s", err), http.StatusBadRequest)
		return
	}

	excludeAlerts, err := parseExcludeAlerts(req)
	if err != nil {
		util_api.RespondError(logger, w, v1.ErrBadData, fmt.Sprintf("invalid parameter %q: %s", "exclude_alerts", err.Error()), http.StatusBadRequest)
		return
	}

	rulesRequest := RulesRequest{
		RuleNames:      req.Form["rule_name[]"],
		RuleGroupNames: req.Form["rule_group[]"],
		Files:          req.Form["file[]"],
		Type:           typ,
		State:          state,
		Health:         health,
		Matchers:       req.Form["match[]"],
		ExcludeAlerts:  excludeAlerts,
	}

	w.Header().Set("Content-Type", "application/json")
	rgs, err := a.ruler.GetRules(req.Context(), rulesRequest)

	if err != nil {
		util_api.RespondError(logger, w, v1.ErrServer, err.Error(), http.StatusInternalServerError)
		return
	}

	groups := make([]*RuleGroup, 0, len(rgs))

	for _, g := range rgs {
		grp := RuleGroup{
			Name:           g.Group.Name,
			File:           g.Group.Namespace,
			Rules:          make([]rule, len(g.ActiveRules)),
			Interval:       g.Group.Interval.Seconds(),
			LastEvaluation: g.GetEvaluationTimestamp(),
			EvaluationTime: g.GetEvaluationDuration().Seconds(),
			Limit:          g.Group.Limit,
		}

		for i, rl := range g.ActiveRules {
			if g.ActiveRules[i].Rule.Alert != "" {
				alerts := make([]*Alert, 0, len(rl.Alerts))
				for _, a := range rl.Alerts {
					alert := &Alert{
						Labels:      cortexpb.FromLabelAdaptersToLabels(a.Labels),
						Annotations: cortexpb.FromLabelAdaptersToLabels(a.Annotations),
						State:       a.GetState(),
						ActiveAt:    &a.ActiveAt,
						Value:       strconv.FormatFloat(a.Value, 'e', -1, 64),
					}
					if !a.KeepFiringSince.IsZero() {
						alert.KeepFiringSince = &a.KeepFiringSince
					}
					alerts = append(alerts, alert)
				}
				grp.Rules[i] = alertingRule{
					State:          rl.GetState(),
					Name:           rl.Rule.GetAlert(),
					Query:          rl.Rule.GetExpr(),
					Duration:       rl.Rule.For.Seconds(),
					Labels:         cortexpb.FromLabelAdaptersToLabels(rl.Rule.Labels),
					Annotations:    cortexpb.FromLabelAdaptersToLabels(rl.Rule.Annotations),
					Alerts:         alerts,
					Health:         rl.GetHealth(),
					LastError:      rl.GetLastError(),
					LastEvaluation: rl.GetEvaluationTimestamp(),
					EvaluationTime: rl.GetEvaluationDuration().Seconds(),
					Type:           v1.RuleTypeAlerting,
					KeepFiringFor:  rl.Rule.KeepFiringFor.Seconds(),
				}
			} else {
				grp.Rules[i] = recordingRule{
					Name:           rl.Rule.GetRecord(),
					Query:          rl.Rule.GetExpr(),
					Labels:         cortexpb.FromLabelAdaptersToLabels(rl.Rule.Labels),
					Health:         rl.GetHealth(),
					LastError:      rl.GetLastError(),
					LastEvaluation: rl.GetEvaluationTimestamp(),
					EvaluationTime: rl.GetEvaluationDuration().Seconds(),
					Type:           v1.RuleTypeRecording,
				}
			}
		}
		groups = append(groups, &grp)
	}

	// keep data.groups are in order
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].File == groups[j].File {
			return groups[i].Name < groups[j].Name
		}
		return groups[i].File < groups[j].File
	})

	b, err := json.Marshal(&util_api.Response{
		Status: "success",
		Data:   &RuleDiscovery{RuleGroups: groups},
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		util_api.RespondError(logger, w, v1.ErrServer, "unable to marshal the requested data", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func parseExcludeAlerts(r *http.Request) (bool, error) {
	excludeAlertsParam := strings.ToLower(r.URL.Query().Get("exclude_alerts"))

	if excludeAlertsParam == "" {
		return false, nil
	}

	excludeAlerts, err := strconv.ParseBool(excludeAlertsParam)
	if err != nil {
		return false, fmt.Errorf("error converting exclude_alerts: %w", err)
	}
	return excludeAlerts, nil
}

func (a *API) PrometheusAlerts(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), a.logger)
	userID, err := tenant.TenantID(req.Context())
	if err != nil || userID == "" {
		level.Error(logger).Log("msg", "error extracting org id from context", "err", err)
		util_api.RespondError(logger, w, v1.ErrBadData, "no valid org id found", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rulesRequest := RulesRequest{
		Type: alertingRuleFilter,
	}
	rgs, err := a.ruler.GetRules(req.Context(), rulesRequest)

	if err != nil {
		util_api.RespondError(logger, w, v1.ErrServer, err.Error(), http.StatusInternalServerError)
		return
	}

	alerts := []*Alert{}

	for _, g := range rgs {
		for _, rl := range g.ActiveRules {
			if rl.Rule.Alert != "" {
				for _, a := range rl.Alerts {
					alert := &Alert{
						Labels:      cortexpb.FromLabelAdaptersToLabels(a.Labels),
						Annotations: cortexpb.FromLabelAdaptersToLabels(a.Annotations),
						State:       a.GetState(),
						ActiveAt:    &a.ActiveAt,
						Value:       strconv.FormatFloat(a.Value, 'e', -1, 64),
					}
					if !a.KeepFiringSince.IsZero() {
						alert.KeepFiringSince = &a.KeepFiringSince
					}
					alerts = append(alerts, alert)
				}
			}
		}
	}

	b, err := json.Marshal(&util_api.Response{
		Status: "success",
		Data:   &AlertDiscovery{Alerts: alerts},
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		util_api.RespondError(logger, w, v1.ErrServer, "unable to marshal the requested data", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

var (
	// ErrNoNamespace signals that no namespace was specified in the request
	ErrNoNamespace = errors.New("a namespace must be provided in the request")
	// ErrNoGroupName signals a group name url parameter was not found
	ErrNoGroupName = errors.New("a matching group name must be provided in the request")
	// ErrNoRuleGroups signals the rule group requested does not exist
	ErrNoRuleGroups = errors.New("no rule groups found")
	// ErrBadRuleGroup is returned when the provided rule group can not be unmarshalled
	ErrBadRuleGroup = errors.New("unable to decoded rule group")
)

func marshalAndSend(output interface{}, w http.ResponseWriter, logger log.Logger) {
	d, err := yaml.Marshal(&output)
	if err != nil {
		level.Error(logger).Log("msg", "error marshalling yaml rule groups", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if _, err := w.Write(d); err != nil {
		level.Error(logger).Log("msg", "error writing yaml response", "err", err)
		return
	}
}

func respondAccepted(w http.ResponseWriter, logger log.Logger) {
	b, err := json.Marshal(&util_api.Response{
		Status: "success",
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		util_api.RespondError(logger, w, v1.ErrServer, "unable to marshal the requested data", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	// Return a status accepted because the rule has been stored and queued for polling, but is not currently active
	w.WriteHeader(http.StatusAccepted)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

// parseNamespace parses the namespace from the provided set of params, in this
// api these params are derived from the url path
func parseNamespace(params map[string]string) (string, error) {
	namespace, exists := params["namespace"]
	if !exists {
		return "", ErrNoNamespace
	}

	namespace, err := url.PathUnescape(namespace)
	if err != nil {
		return "", err
	}

	return namespace, nil
}

// parseGroupName parses the group name from the provided set of params, in this
// api these params are derived from the url path
func parseGroupName(params map[string]string) (string, error) {
	groupName, exists := params["groupName"]
	if !exists {
		return "", ErrNoGroupName
	}

	groupName, err := url.PathUnescape(groupName)
	if err != nil {
		return "", err
	}

	return groupName, nil
}

// parseRequest parses the incoming request to parse out the userID, rules namespace, and rule group name
// and returns them in that order. It also allows users to require a namespace or group name and return
// an error if it they can not be parsed.
func parseRequest(req *http.Request, requireNamespace, requireGroup bool) (string, string, string, error) {
	userID, err := tenant.TenantID(req.Context())
	if err != nil {
		return "", "", "", user.ErrNoOrgID
	}

	vars := mux.Vars(req)

	namespace, err := parseNamespace(vars)
	if err != nil {
		if err != ErrNoNamespace || requireNamespace {
			return "", "", "", err
		}
	}

	group, err := parseGroupName(vars)
	if err != nil {
		if err != ErrNoGroupName || requireGroup {
			return "", "", "", err
		}
	}

	return userID, namespace, group, nil
}

func (a *API) ListRules(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), a.logger)

	userID, namespace, _, err := parseRequest(req, false, false)
	if err != nil {
		util_api.RespondError(logger, w, v1.ErrBadData, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(logger).Log("msg", "retrieving rule groups with namespace", "userID", userID, "namespace", namespace)
	rgs, err := a.store.ListRuleGroupsForUserAndNamespace(req.Context(), userID, namespace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(rgs) == 0 {
		level.Info(logger).Log("msg", "no rule groups found", "userID", userID)
		http.Error(w, ErrNoRuleGroups.Error(), http.StatusNotFound)
		return
	}

	_, err = a.store.LoadRuleGroups(req.Context(), map[string]rulespb.RuleGroupList{userID: rgs})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(logger).Log("msg", "retrieved rule groups from rule store", "userID", userID, "num_namespaces", len(rgs))

	formatted := rgs.Formatted()
	marshalAndSend(formatted, w, logger)
}

func (a *API) GetRuleGroup(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), a.logger)
	userID, namespace, groupName, err := parseRequest(req, true, true)
	if err != nil {
		util_api.RespondError(logger, w, v1.ErrBadData, err.Error(), http.StatusBadRequest)
		return
	}

	rg, err := a.store.GetRuleGroup(req.Context(), userID, namespace, groupName)
	if err != nil {
		if errors.Is(err, rulestore.ErrGroupNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	formatted := rulespb.FromProto(rg)
	marshalAndSend(formatted, w, logger)
}

func (a *API) CreateRuleGroup(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), a.logger)
	userID, namespace, _, err := parseRequest(req, true, false)
	if err != nil {
		util_api.RespondError(logger, w, v1.ErrBadData, err.Error(), http.StatusBadRequest)
		return
	}

	payload, err := io.ReadAll(req.Body)
	if err != nil {
		level.Error(logger).Log("msg", "unable to read rule group payload", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(logger).Log("msg", "attempting to unmarshal rulegroup", "userID", userID, "group", string(payload))

	rg := rulefmt.RuleGroup{}
	err = yaml.Unmarshal(payload, &rg)
	if err != nil {
		level.Error(logger).Log("msg", "unable to unmarshal rule group payload", "err", err.Error())
		http.Error(w, ErrBadRuleGroup.Error(), http.StatusBadRequest)
		return
	}

	errs := a.ruler.manager.ValidateRuleGroup(rg)
	if len(errs) > 0 {
		e := []string{}
		for _, err := range errs {
			level.Error(logger).Log("msg", "unable to validate rule group payload", "err", err.Error())
			e = append(e, err.Error())
		}

		http.Error(w, strings.Join(e, ", "), http.StatusBadRequest)
		return
	}

	if err := a.ruler.AssertMaxRulesPerRuleGroup(userID, len(rg.Rules)); err != nil {
		level.Error(logger).Log("msg", "limit validation failure", "err", err.Error(), "user", userID)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if a.ruler.HasMaxRuleGroupsLimit(userID) {
		rgs, err := a.store.ListRuleGroupsForUserAndNamespace(req.Context(), userID, "")
		if err != nil {
			level.Error(logger).Log("msg", "unable to fetch current rule groups for validation", "err", err.Error(), "user", userID)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := a.ruler.AssertMaxRuleGroups(userID, len(rgs)+1); err != nil {
			level.Error(logger).Log("msg", "limit validation failure", "err", err.Error(), "user", userID)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	rgProto := rulespb.ToProto(userID, namespace, rg)
	loadedRg := rulespb.FromProto(rgProto)
	rgYaml, err := yaml.Marshal(loadedRg)
	if err == nil {
		err = yaml.Unmarshal(rgYaml, &rulefmt.RuleGroup{})
	}
	if err != nil {
		level.Error(logger).Log("msg", "unable to load rule group from proto", "err", err.Error(), "user", userID)
		http.Error(w, ErrBadRuleGroup.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(logger).Log("msg", "attempting to store rulegroup", "userID", userID, "group", rgProto.String())
	err = a.store.SetRuleGroup(req.Context(), userID, namespace, rgProto)
	if err != nil {
		level.Error(logger).Log("msg", "unable to store rule group", "err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondAccepted(w, logger)
}

func (a *API) DeleteNamespace(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), a.logger)

	userID, namespace, _, err := parseRequest(req, true, false)
	if err != nil {
		util_api.RespondError(logger, w, v1.ErrBadData, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.store.DeleteNamespace(req.Context(), userID, namespace)
	if err != nil {
		if err == rulestore.ErrGroupNamespaceNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		util_api.RespondError(logger, w, v1.ErrServer, err.Error(), http.StatusInternalServerError)
		return
	}

	respondAccepted(w, logger)
}

func (a *API) DeleteRuleGroup(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), a.logger)

	userID, namespace, groupName, err := parseRequest(req, true, true)
	if err != nil {
		util_api.RespondError(logger, w, v1.ErrBadData, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.store.DeleteRuleGroup(req.Context(), userID, namespace, groupName)
	if err != nil {
		if err == rulestore.ErrGroupNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		util_api.RespondError(logger, w, v1.ErrServer, err.Error(), http.StatusInternalServerError)
		return
	}

	respondAccepted(w, logger)
}
