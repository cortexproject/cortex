// Copyright 2015 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/dispatch"
	"github.com/prometheus/alertmanager/pkg/parse"
	"github.com/prometheus/alertmanager/provider"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/silence/silencepb"
	"github.com/prometheus/alertmanager/types"
	"github.com/weaveworks/mesh"
)

var (
	numReceivedAlerts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "alertmanager",
		Name:      "alerts_received_total",
		Help:      "The total number of received alerts.",
	}, []string{"status"})

	numInvalidAlerts = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "alertmanager",
		Name:      "alerts_invalid_total",
		Help:      "The total number of received alerts that were invalid.",
	})
)

func init() {
	prometheus.Register(numReceivedAlerts)
	prometheus.Register(numInvalidAlerts)
}

var corsHeaders = map[string]string{
	"Access-Control-Allow-Headers":  "Accept, Authorization, Content-Type, Origin",
	"Access-Control-Allow-Methods":  "GET, DELETE, OPTIONS",
	"Access-Control-Allow-Origin":   "*",
	"Access-Control-Expose-Headers": "Date",
}

// Enables cross-site script calls.
func setCORS(w http.ResponseWriter) {
	for h, v := range corsHeaders {
		w.Header().Set(h, v)
	}
}

// API provides registration of handlers for API routes.
type API struct {
	alerts         provider.Alerts
	silences       *silence.Silences
	config         *config.Config
	route          *dispatch.Route
	resolveTimeout time.Duration
	uptime         time.Time
	mrouter        *mesh.Router
	logger         log.Logger

	groups         groupsFn
	getAlertStatus getAlertStatusFn

	mtx sync.RWMutex
}

type groupsFn func([]*labels.Matcher) dispatch.AlertOverview
type getAlertStatusFn func(model.Fingerprint) types.AlertStatus

// New returns a new API.
func New(alerts provider.Alerts, silences *silence.Silences, gf groupsFn, sf getAlertStatusFn, router *mesh.Router, l log.Logger) *API {
	return &API{
		alerts:         alerts,
		silences:       silences,
		groups:         gf,
		getAlertStatus: sf,
		uptime:         time.Now(),
		mrouter:        router,
		logger:         l,
	}
}

// Register registers the API handlers under their correct routes
// in the given router.
func (api *API) Register(r *route.Router) {
	ihf := func(name string, f http.HandlerFunc) http.HandlerFunc {
		return InstrumentHandlerFunc(name, func(w http.ResponseWriter, r *http.Request) {
			setCORS(w)
			f(w, r)
		})
	}

	r.Options("/*path", ihf("options", func(w http.ResponseWriter, r *http.Request) {}))

	// Register legacy forwarder for alert pushing.
	r.Post("/alerts", ihf("legacy_add_alerts", api.legacyAddAlerts))

	// Register actual API.
	r = r.WithPrefix("/v1")

	r.Get("/status", ihf("status", api.status))
	r.Get("/receivers", ihf("receivers", api.receivers))
	r.Get("/alerts/groups", ihf("alert_groups", api.alertGroups))

	r.Get("/alerts", ihf("list_alerts", api.listAlerts))
	r.Post("/alerts", ihf("add_alerts", api.addAlerts))

	r.Get("/silences", ihf("list_silences", api.listSilences))
	r.Post("/silences", ihf("add_silence", api.setSilence))
	r.Get("/silence/:sid", ihf("get_silence", api.getSilence))
	r.Del("/silence/:sid", ihf("del_silence", api.delSilence))
}

// Update sets the configuration string to a new value.
func (api *API) Update(cfg *config.Config, resolveTimeout time.Duration) error {
	api.mtx.Lock()
	defer api.mtx.Unlock()

	api.resolveTimeout = resolveTimeout
	api.config = cfg
	api.route = dispatch.NewRoute(cfg.Route, nil)
	return nil
}

type errorType string

const (
	errorNone     errorType = ""
	errorInternal           = "server_error"
	errorBadData            = "bad_data"
)

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

func (api *API) receivers(w http.ResponseWriter, req *http.Request) {
	api.mtx.RLock()
	defer api.mtx.RUnlock()

	receivers := make([]string, 0, len(api.config.Receivers))
	for _, r := range api.config.Receivers {
		receivers = append(receivers, r.Name)
	}

	api.respond(w, receivers)
}

func (api *API) status(w http.ResponseWriter, req *http.Request) {
	api.mtx.RLock()

	var status = struct {
		ConfigYAML  string            `json:"configYAML"`
		ConfigJSON  *config.Config    `json:"configJSON"`
		VersionInfo map[string]string `json:"versionInfo"`
		Uptime      time.Time         `json:"uptime"`
		MeshStatus  *meshStatus       `json:"meshStatus"`
	}{
		ConfigYAML: api.config.String(),
		ConfigJSON: api.config,
		VersionInfo: map[string]string{
			"version":   version.Version,
			"revision":  version.Revision,
			"branch":    version.Branch,
			"buildUser": version.BuildUser,
			"buildDate": version.BuildDate,
			"goVersion": version.GoVersion,
		},
		Uptime:     api.uptime,
		MeshStatus: getMeshStatus(api),
	}

	api.mtx.RUnlock()

	api.respond(w, status)
}

type meshStatus struct {
	Name        string             `json:"name"`
	NickName    string             `json:"nickName"`
	Peers       []peerStatus       `json:"peers"`
	Connections []connectionStatus `json:"connections"`
}

type peerStatus struct {
	Name     string `json:"name"`     // e.g. "00:00:00:00:00:01"
	NickName string `json:"nickName"` // e.g. "a"
	UID      uint64 `json:"uid"`      // e.g. "14015114173033265000"
}

type connectionStatus struct {
	Address  string `json:"address"`
	Outbound bool   `json:"outbound"`
	State    string `json:"state"`
	Info     string `json:"info"`
}

func getMeshStatus(api *API) *meshStatus {
	if api.mrouter == nil {
		return nil
	}

	status := mesh.NewStatus(api.mrouter)
	strippedStatus := &meshStatus{
		Name:        status.Name,
		NickName:    status.NickName,
		Peers:       make([]peerStatus, len(status.Peers)),
		Connections: make([]connectionStatus, len(status.Connections)),
	}

	for i := 0; i < len(status.Peers); i++ {
		strippedStatus.Peers[i] = peerStatus{
			Name:     status.Peers[i].Name,
			NickName: status.Peers[i].NickName,
			UID:      uint64(status.Peers[i].UID),
		}
	}
	for i := 0; i < len(status.Connections); i++ {
		strippedStatus.Connections[i] = connectionStatus{
			Address:  status.Connections[i].Address,
			Outbound: status.Connections[i].Outbound,
			State:    status.Connections[i].State,
			Info:     status.Connections[i].Info,
		}
	}

	return strippedStatus
}

func (api *API) alertGroups(w http.ResponseWriter, r *http.Request) {
	var err error
	matchers := []*labels.Matcher{}

	if filter := r.FormValue("filter"); filter != "" {
		matchers, err = parse.Matchers(filter)
		if err != nil {
			api.respondError(w, apiError{
				typ: errorBadData,
				err: err,
			}, nil)
			return
		}
	}

	groups := api.groups(matchers)

	api.respond(w, groups)
}

func (api *API) listAlerts(w http.ResponseWriter, r *http.Request) {
	var (
		err            error
		receiverFilter *regexp.Regexp
		// Initialize result slice to prevent api returning `null` when there
		// are no alerts present
		res           = []*dispatch.APIAlert{}
		matchers      = []*labels.Matcher{}
		showSilenced  = true
		showInhibited = true
	)

	if filter := r.FormValue("filter"); filter != "" {
		matchers, err = parse.Matchers(filter)
		if err != nil {
			api.respondError(w, apiError{
				typ: errorBadData,
				err: err,
			}, nil)
			return
		}
	}

	if silencedParam := r.FormValue("silenced"); silencedParam != "" {
		if silencedParam == "false" {
			showSilenced = false
		} else if silencedParam != "true" {
			api.respondError(w, apiError{
				typ: errorBadData,
				err: fmt.Errorf(
					"parameter 'silenced' can either be 'true' or 'false', not '%v'",
					silencedParam,
				),
			}, nil)
			return
		}
	}

	if inhibitedParam := r.FormValue("inhibited"); inhibitedParam != "" {
		if inhibitedParam == "false" {
			showInhibited = false
		} else if inhibitedParam != "true" {
			api.respondError(w, apiError{
				typ: errorBadData,
				err: fmt.Errorf(
					"parameter 'inhibited' can either be 'true' or 'false', not '%v'",
					inhibitedParam,
				),
			}, nil)
			return
		}
	}

	if receiverParam := r.FormValue("receiver"); receiverParam != "" {
		receiverFilter, err = regexp.Compile("^(?:" + receiverParam + ")$")
		if err != nil {
			api.respondError(w, apiError{
				typ: errorBadData,
				err: fmt.Errorf(
					"failed to parse receiver param: %s",
					receiverParam,
				),
			}, nil)
			return
		}
	}

	alerts := api.alerts.GetPending()
	defer alerts.Close()

	api.mtx.RLock()
	// TODO(fabxc): enforce a sensible timeout.
	for a := range alerts.Next() {
		if err = alerts.Err(); err != nil {
			break
		}

		routes := api.route.Match(a.Labels)
		receivers := make([]string, 0, len(routes))
		for _, r := range routes {
			receivers = append(receivers, r.RouteOpts.Receiver)
		}

		if receiverFilter != nil && !receiversMatchFilter(receivers, receiverFilter) {
			continue
		}

		if !alertMatchesFilterLabels(&a.Alert, matchers) {
			continue
		}

		// Continue if alert is resolved
		if !a.Alert.EndsAt.IsZero() && a.Alert.EndsAt.Before(time.Now()) {
			continue
		}

		status := api.getAlertStatus(a.Fingerprint())

		if !showSilenced && len(status.SilencedBy) != 0 {
			continue
		}

		if !showInhibited && len(status.InhibitedBy) != 0 {
			continue
		}

		apiAlert := &dispatch.APIAlert{
			Alert:       &a.Alert,
			Status:      status,
			Receivers:   receivers,
			Fingerprint: a.Fingerprint().String(),
		}

		res = append(res, apiAlert)
	}
	api.mtx.RUnlock()

	if err != nil {
		api.respondError(w, apiError{
			typ: errorInternal,
			err: err,
		}, nil)
		return
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Fingerprint < res[j].Fingerprint
	})
	api.respond(w, res)
}

func receiversMatchFilter(receivers []string, filter *regexp.Regexp) bool {
	for _, r := range receivers {
		if filter.MatchString(r) {
			return true
		}
	}

	return false
}

func alertMatchesFilterLabels(a *model.Alert, matchers []*labels.Matcher) bool {
	sms := make(map[string]string)
	for name, value := range a.Labels {
		sms[string(name)] = string(value)
	}
	return matchFilterLabels(matchers, sms)
}

func (api *API) legacyAddAlerts(w http.ResponseWriter, r *http.Request) {
	var legacyAlerts = []struct {
		Summary     model.LabelValue `json:"summary"`
		Description model.LabelValue `json:"description"`
		Runbook     model.LabelValue `json:"runbook"`
		Labels      model.LabelSet   `json:"labels"`
		Payload     model.LabelSet   `json:"payload"`
	}{}
	if err := api.receive(r, &legacyAlerts); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var alerts []*types.Alert
	for _, la := range legacyAlerts {
		a := &types.Alert{
			Alert: model.Alert{
				Labels:      la.Labels,
				Annotations: la.Payload,
			},
		}
		if a.Annotations == nil {
			a.Annotations = model.LabelSet{}
		}
		a.Annotations["summary"] = la.Summary
		a.Annotations["description"] = la.Description
		a.Annotations["runbook"] = la.Runbook

		alerts = append(alerts, a)
	}

	api.insertAlerts(w, r, alerts...)
}

func (api *API) addAlerts(w http.ResponseWriter, r *http.Request) {
	var alerts []*types.Alert
	if err := api.receive(r, &alerts); err != nil {
		api.respondError(w, apiError{
			typ: errorBadData,
			err: err,
		}, nil)
		return
	}

	api.insertAlerts(w, r, alerts...)
}

func (api *API) insertAlerts(w http.ResponseWriter, r *http.Request, alerts ...*types.Alert) {
	now := time.Now()

	api.mtx.RLock()
	resolveTimeout := api.resolveTimeout
	api.mtx.RUnlock()

	for _, alert := range alerts {
		alert.UpdatedAt = now

		// Ensure StartsAt is set.
		if alert.StartsAt.IsZero() {
			alert.StartsAt = now
		}
		// If no end time is defined, set a timeout after which an alert
		// is marked resolved if it is not updated.
		if alert.EndsAt.IsZero() {
			alert.Timeout = true
			alert.EndsAt = now.Add(resolveTimeout)

			numReceivedAlerts.WithLabelValues("firing").Inc()
		} else {
			numReceivedAlerts.WithLabelValues("resolved").Inc()
		}
	}

	// Make a best effort to insert all alerts that are valid.
	var (
		validAlerts    = make([]*types.Alert, 0, len(alerts))
		validationErrs = &types.MultiError{}
	)
	for _, a := range alerts {
		if err := a.Validate(); err != nil {
			validationErrs.Add(err)
			numInvalidAlerts.Inc()
			continue
		}
		validAlerts = append(validAlerts, a)
	}
	if err := api.alerts.Put(validAlerts...); err != nil {
		api.respondError(w, apiError{
			typ: errorInternal,
			err: err,
		}, nil)
		return
	}

	if validationErrs.Len() > 0 {
		api.respondError(w, apiError{
			typ: errorBadData,
			err: validationErrs,
		}, nil)
		return
	}

	api.respond(w, nil)
}

func (api *API) setSilence(w http.ResponseWriter, r *http.Request) {
	var sil types.Silence
	if err := api.receive(r, &sil); err != nil {
		api.respondError(w, apiError{
			typ: errorBadData,
			err: err,
		}, nil)
		return
	}
	psil, err := silenceToProto(&sil)
	if err != nil {
		api.respondError(w, apiError{
			typ: errorBadData,
			err: err,
		}, nil)
		return
	}

	sid, err := api.silences.Set(psil)
	if err != nil {
		api.respondError(w, apiError{
			typ: errorBadData,
			err: err,
		}, nil)
		return
	}

	api.respond(w, struct {
		SilenceID string `json:"silenceId"`
	}{
		SilenceID: sid,
	})
}

func (api *API) getSilence(w http.ResponseWriter, r *http.Request) {
	sid := route.Param(r.Context(), "sid")

	sils, err := api.silences.Query(silence.QIDs(sid))
	if err != nil || len(sils) == 0 {
		http.Error(w, fmt.Sprint("Error getting silence: ", err), http.StatusNotFound)
		return
	}
	sil, err := silenceFromProto(sils[0])
	if err != nil {
		api.respondError(w, apiError{
			typ: errorInternal,
			err: err,
		}, nil)
		return
	}

	api.respond(w, sil)
}

func (api *API) delSilence(w http.ResponseWriter, r *http.Request) {
	sid := route.Param(r.Context(), "sid")

	if err := api.silences.Expire(sid); err != nil {
		api.respondError(w, apiError{
			typ: errorBadData,
			err: err,
		}, nil)
		return
	}
	api.respond(w, nil)
}

func (api *API) listSilences(w http.ResponseWriter, r *http.Request) {
	psils, err := api.silences.Query()
	if err != nil {
		api.respondError(w, apiError{
			typ: errorInternal,
			err: err,
		}, nil)
		return
	}

	matchers := []*labels.Matcher{}
	if filter := r.FormValue("filter"); filter != "" {
		matchers, err = parse.Matchers(filter)
		if err != nil {
			api.respondError(w, apiError{
				typ: errorBadData,
				err: err,
			}, nil)
			return
		}
	}

	sils := []*types.Silence{}
	for _, ps := range psils {
		s, err := silenceFromProto(ps)
		if err != nil {
			api.respondError(w, apiError{
				typ: errorInternal,
				err: err,
			}, nil)
			return
		}

		if !silenceMatchesFilterLabels(s, matchers) {
			continue
		}
		sils = append(sils, s)
	}

	var active, pending, expired []*types.Silence

	for _, s := range sils {
		switch s.Status.State {
		case types.SilenceStateActive:
			active = append(active, s)
		case types.SilenceStatePending:
			pending = append(pending, s)
		case types.SilenceStateExpired:
			expired = append(expired, s)
		}
	}

	sort.Slice(active, func(i int, j int) bool {
		return active[i].EndsAt.Before(active[j].EndsAt)
	})
	sort.Slice(pending, func(i int, j int) bool {
		return pending[i].StartsAt.Before(pending[j].EndsAt)
	})
	sort.Slice(expired, func(i int, j int) bool {
		return expired[i].EndsAt.After(expired[j].EndsAt)
	})

	// Initialize silences explicitly to an empty list (instead of nil)
	// So that it does not get converted to "null" in JSON.
	silences := []*types.Silence{}
	silences = append(silences, active...)
	silences = append(silences, pending...)
	silences = append(silences, expired...)

	api.respond(w, silences)
}

func silenceMatchesFilterLabels(s *types.Silence, matchers []*labels.Matcher) bool {
	sms := make(map[string]string)
	for _, m := range s.Matchers {
		sms[m.Name] = m.Value
	}

	return matchFilterLabels(matchers, sms)
}

func matchFilterLabels(matchers []*labels.Matcher, sms map[string]string) bool {
	for _, m := range matchers {
		v, prs := sms[m.Name]
		switch m.Type {
		case labels.MatchNotEqual, labels.MatchNotRegexp:
			if !m.Matches(string(v)) {
				return false
			}
		default:
			if !prs || !m.Matches(string(v)) {
				return false
			}
		}
	}

	return true
}

func silenceToProto(s *types.Silence) (*silencepb.Silence, error) {
	sil := &silencepb.Silence{
		Id:        s.ID,
		StartsAt:  s.StartsAt,
		EndsAt:    s.EndsAt,
		UpdatedAt: s.UpdatedAt,
		Comment:   s.Comment,
		CreatedBy: s.CreatedBy,
	}
	for _, m := range s.Matchers {
		matcher := &silencepb.Matcher{
			Name:    m.Name,
			Pattern: m.Value,
			Type:    silencepb.Matcher_EQUAL,
		}
		if m.IsRegex {
			matcher.Type = silencepb.Matcher_REGEXP
		}
		sil.Matchers = append(sil.Matchers, matcher)
	}
	return sil, nil
}

func silenceFromProto(s *silencepb.Silence) (*types.Silence, error) {
	sil := &types.Silence{
		ID:        s.Id,
		StartsAt:  s.StartsAt,
		EndsAt:    s.EndsAt,
		UpdatedAt: s.UpdatedAt,
		Status: types.SilenceStatus{
			State: types.CalcSilenceState(s.StartsAt, s.EndsAt),
		},
		Comment:   s.Comment,
		CreatedBy: s.CreatedBy,
	}
	for _, m := range s.Matchers {
		matcher := &types.Matcher{
			Name:  m.Name,
			Value: m.Pattern,
		}
		switch m.Type {
		case silencepb.Matcher_EQUAL:
		case silencepb.Matcher_REGEXP:
			matcher.IsRegex = true
		default:
			return nil, fmt.Errorf("unknown matcher type")
		}
		sil.Matchers = append(sil.Matchers, matcher)
	}

	return sil, nil
}

type status string

const (
	statusSuccess status = "success"
	statusError          = "error"
)

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

func (api *API) respond(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)

	b, err := json.Marshal(&response{
		Status: statusSuccess,
		Data:   data,
	})
	if err != nil {
		level.Error(api.logger).Log("msg", "Error marshalling JSON", "err", err)
		return
	}
	w.Write(b)
}

func (api *API) respondError(w http.ResponseWriter, apiErr apiError, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	switch apiErr.typ {
	case errorBadData:
		w.WriteHeader(http.StatusBadRequest)
	case errorInternal:
		w.WriteHeader(http.StatusInternalServerError)
	default:
		panic(fmt.Sprintf("unknown error type %q", apiErr))
	}

	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		return
	}
	level.Error(api.logger).Log("msg", "API error", "err", apiErr.Error())

	w.Write(b)
}

func (api *API) receive(r *http.Request, v interface{}) error {
	dec := json.NewDecoder(r.Body)
	defer r.Body.Close()

	err := dec.Decode(v)
	if err != nil {
		level.Debug(api.logger).Log("msg", "Decoding request failed", "err", err)
	}
	return err
}

// InstrumentHandlerFunc wraps the given function for instrumentation. It
// otherwise works in the same way as InstrumentHandler (and shares the same
// issues).
//
// Deprecated: InstrumentHandlerFunc has several issues:
//
// - It uses Summaries rather than Histograms. Summaries are not useful if
// aggregation across multiple instances is required.
//
// - It uses microseconds as unit, which is deprecated and should be replaced by
// seconds.
//
// - The size of the request is calculated in a separate goroutine. Since this
// calculator requires access to the request header, it creates a race with
// any writes to the header performed during request handling.
// httputil.ReverseProxy is a prominent example for a handler
// performing such writes.
//
// Upcoming versions of this package will provide ways of instrumenting HTTP
// handlers that are more flexible and have fewer issues. Please prefer direct
// instrumentation in the meantime.
func InstrumentHandlerFunc(handlerName string, handlerFunc func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return InstrumentHandlerFuncWithOpts(
		prometheus.SummaryOpts{
			Subsystem:   "http",
			ConstLabels: prometheus.Labels{"handler": handlerName},
		},
		handlerFunc,
	)
}

// InstrumentHandlerFuncWithOpts works like InstrumentHandlerFunc (and shares
// the same issues) but provides more flexibility (at the cost of a more complex
// call syntax). See InstrumentHandlerWithOpts for details how the provided
// SummaryOpts are used.
//
// Deprecated: InstrumentHandlerFuncWithOpts is deprecated for the same reasons
// as InstrumentHandlerFunc is.
func InstrumentHandlerFuncWithOpts(opts prometheus.SummaryOpts, handlerFunc func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	reqCnt := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   opts.Namespace,
			Subsystem:   opts.Subsystem,
			Name:        "requests_total",
			Help:        "Total number of HTTP requests made.",
			ConstLabels: opts.ConstLabels,
		},
		instLabels,
	)

	opts.Name = "request_duration_microseconds"
	opts.Help = "The HTTP request latencies in microseconds."
	reqDur := prometheus.NewSummary(opts)

	opts.Name = "request_size_bytes"
	opts.Help = "The HTTP request sizes in bytes."
	reqSz := prometheus.NewSummary(opts)

	opts.Name = "response_size_bytes"
	opts.Help = "The HTTP response sizes in bytes."
	resSz := prometheus.NewSummary(opts)

	regReqCnt := MustRegisterOrGet(reqCnt).(*prometheus.CounterVec)
	regReqDur := MustRegisterOrGet(reqDur).(prometheus.Summary)
	regReqSz := MustRegisterOrGet(reqSz).(prometheus.Summary)
	regResSz := MustRegisterOrGet(resSz).(prometheus.Summary)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()

		delegate := &responseWriterDelegator{ResponseWriter: w}
		out := make(chan int)
		urlLen := 0
		if r.URL != nil {
			urlLen = len(r.URL.String())
		}
		go computeApproximateRequestSize(r, out, urlLen)

		_, cn := w.(http.CloseNotifier)
		_, fl := w.(http.Flusher)
		_, hj := w.(http.Hijacker)
		_, rf := w.(io.ReaderFrom)
		var rw http.ResponseWriter
		if cn && fl && hj && rf {
			rw = &fancyResponseWriterDelegator{delegate}
		} else {
			rw = delegate
		}
		handlerFunc(rw, r)

		elapsed := float64(time.Since(now)) / float64(time.Microsecond)

		method := sanitizeMethod(r.Method)
		code := sanitizeCode(delegate.status)
		regReqCnt.WithLabelValues(method, code).Inc()
		regReqDur.Observe(elapsed)
		regResSz.Observe(float64(delegate.written))
		regReqSz.Observe(float64(<-out))
	})
}

func computeApproximateRequestSize(r *http.Request, out chan int, s int) {
	s += len(r.Method)
	s += len(r.Proto)
	for name, values := range r.Header {
		s += len(name)
		for _, value := range values {
			s += len(value)
		}
	}
	s += len(r.Host)

	// N.B. r.Form and r.MultipartForm are assumed to be included in r.URL.

	if r.ContentLength != -1 {
		s += int(r.ContentLength)
	}
	out <- s
}

func sanitizeMethod(m string) string {
	switch m {
	case "GET", "get":
		return "get"
	case "PUT", "put":
		return "put"
	case "HEAD", "head":
		return "head"
	case "POST", "post":
		return "post"
	case "DELETE", "delete":
		return "delete"
	case "CONNECT", "connect":
		return "connect"
	case "OPTIONS", "options":
		return "options"
	case "NOTIFY", "notify":
		return "notify"
	default:
		return strings.ToLower(m)
	}
}

var instLabels = []string{"method", "code"}

type responseWriterDelegator struct {
	http.ResponseWriter

	handler, method string
	status          int
	written         int64
	wroteHeader     bool
}

func (r *responseWriterDelegator) WriteHeader(code int) {
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseWriterDelegator) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.written += int64(n)
	return n, err
}

type fancyResponseWriterDelegator struct {
	*responseWriterDelegator
}

func (f *fancyResponseWriterDelegator) CloseNotify() <-chan bool {
	return f.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

func (f *fancyResponseWriterDelegator) Flush() {
	f.ResponseWriter.(http.Flusher).Flush()
}

func (f *fancyResponseWriterDelegator) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return f.ResponseWriter.(http.Hijacker).Hijack()
}

func (f *fancyResponseWriterDelegator) ReadFrom(r io.Reader) (int64, error) {
	if !f.wroteHeader {
		f.WriteHeader(http.StatusOK)
	}
	n, err := f.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	f.written += n
	return n, err
}

func sanitizeCode(s int) string {
	switch s {
	case 100:
		return "100"
	case 101:
		return "101"

	case 200:
		return "200"
	case 201:
		return "201"
	case 202:
		return "202"
	case 203:
		return "203"
	case 204:
		return "204"
	case 205:
		return "205"
	case 206:
		return "206"

	case 300:
		return "300"
	case 301:
		return "301"
	case 302:
		return "302"
	case 304:
		return "304"
	case 305:
		return "305"
	case 307:
		return "307"

	case 400:
		return "400"
	case 401:
		return "401"
	case 402:
		return "402"
	case 403:
		return "403"
	case 404:
		return "404"
	case 405:
		return "405"
	case 406:
		return "406"
	case 407:
		return "407"
	case 408:
		return "408"
	case 409:
		return "409"
	case 410:
		return "410"
	case 411:
		return "411"
	case 412:
		return "412"
	case 413:
		return "413"
	case 414:
		return "414"
	case 415:
		return "415"
	case 416:
		return "416"
	case 417:
		return "417"
	case 418:
		return "418"

	case 500:
		return "500"
	case 501:
		return "501"
	case 502:
		return "502"
	case 503:
		return "503"
	case 504:
		return "504"
	case 505:
		return "505"

	case 428:
		return "428"
	case 429:
		return "429"
	case 431:
		return "431"
	case 511:
		return "511"

	default:
		return strconv.Itoa(s)
	}
}

// RegisterOrGet registers the provided Collector with the DefaultRegisterer and
// returns the Collector, unless an equal Collector was registered before, in
// which case that Collector is returned.
func RegisterOrGet(c prometheus.Collector) (prometheus.Collector, error) {
	if err := prometheus.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector, nil
		}
		return nil, err
	}
	return c, nil
}

// MustRegisterOrGet behaves like RegisterOrGet but panics instead of returning
// an error.
func MustRegisterOrGet(c prometheus.Collector) prometheus.Collector {
	c, err := RegisterOrGet(c)
	if err != nil {
		panic(err)
	}
	return c
}
