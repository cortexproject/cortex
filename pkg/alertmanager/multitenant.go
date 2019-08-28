package alertmanager

import (
	"context"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/alertmanager/cluster"
	amconfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/configs"
	configs_client "github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var backoffConfig = util.BackoffConfig{
	// Backoff for loading initial configuration set.
	MinBackoff: 100 * time.Millisecond,
	MaxBackoff: 2 * time.Second,
}

const (
	// If a config sets the webhook URL to this, it will be rewritten to
	// a URL derived from Config.AutoWebhookRoot
	autoWebhookURL = "http://internal.monitor"

	statusPage = `
<!doctype html>
<html>
	<head><title>Cortex Alertmanager Status</title></head>
	<body>
		<h1>Cortex Alertmanager Status</h1>
		<h2>Node</h2>
		<dl>
			<dt>Name</dt><dd>{{.self.Name}}</dd>
			<dt>Addr</dt><dd>{{.self.Addr}}</dd>
			<dt>Port</dt><dd>{{.self.Port}}</dd>
		</dl>
		<h3>Members</h3>
		{{ with .members }}
		<table>
		<tr><th>Name</th><th>Addr</th></tr>
		{{ range . }}
		<tr><td>{{ .Name }}</td><td>{{ .Addr }}</td></tr>
		{{ end }}
		</table>
		{{ else }}
		<p>No peers</p>
		{{ end }}
	</body>
</html>
`
)

var (
	totalConfigs = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "alertmanager_configs_total",
		Help:      "How many configs the multitenant alertmanager knows about.",
	})
	statusTemplate      *template.Template
	allConnectionStates = []string{"established", "pending", "retrying", "failed", "connecting"}
)

func init() {
	prometheus.MustRegister(totalConfigs)
	statusTemplate = template.Must(template.New("statusPage").Funcs(map[string]interface{}{
		"state": func(enabled bool) string {
			if enabled {
				return "enabled"
			}
			return "disabled"
		},
	}).Parse(statusPage))
}

// Print counts in a specified order
func counts(counts map[string]int, keys []string) string {
	var stringCounts []string
	for _, key := range keys {
		if count, ok := counts[key]; ok {
			stringCounts = append(stringCounts, fmt.Sprintf("%d %s", count, key))
		}
	}
	return strings.Join(stringCounts, ", ")
}

// MultitenantAlertmanagerConfig is the configuration for a multitenant Alertmanager.
type MultitenantAlertmanagerConfig struct {
	DataDir      string
	Retention    time.Duration
	ExternalURL  flagext.URLValue
	PollInterval time.Duration

	clusterBindAddr      string
	clusterAdvertiseAddr string
	peers                flagext.StringSlice
	peerTimeout          time.Duration

	FallbackConfigFile string
	AutoWebhookRoot    string
}

const defaultClusterAddr = "0.0.0.0:9094"

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *MultitenantAlertmanagerConfig) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.DataDir, "alertmanager.storage.path", "data/", "Base path for data storage.")
	flag.DurationVar(&cfg.Retention, "alertmanager.storage.retention", 5*24*time.Hour, "How long to keep data for.")

	flag.Var(&cfg.ExternalURL, "alertmanager.web.external-url", "The URL under which Alertmanager is externally reachable (for example, if Alertmanager is served via a reverse proxy). Used for generating relative and absolute links back to Alertmanager itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Alertmanager. If omitted, relevant URL components will be derived automatically.")

	flag.StringVar(&cfg.FallbackConfigFile, "alertmanager.configs.fallback", "", "Filename of fallback config to use if none specified for instance.")
	flag.StringVar(&cfg.AutoWebhookRoot, "alertmanager.configs.auto-webhook-root", "", "Root of URL to generate if config is "+autoWebhookURL)
	flag.DurationVar(&cfg.PollInterval, "alertmanager.configs.poll-interval", 15*time.Second, "How frequently to poll Cortex configs")

	flag.StringVar(&cfg.clusterBindAddr, "cluster.listen-address", defaultClusterAddr, "Listen address for cluster.")
	flag.StringVar(&cfg.clusterAdvertiseAddr, "cluster.advertise-address", "", "Explicit address to advertise in cluster.")
	flag.Var(&cfg.peers, "cluster.peer", "Initial peers (may be repeated).")
	flag.DurationVar(&cfg.peerTimeout, "cluster.peer-timeout", time.Second*15, "Time to wait between peers to send notifications.")
}

// A MultitenantAlertmanager manages Alertmanager instances for multiple
// organizations.
type MultitenantAlertmanager struct {
	cfg *MultitenantAlertmanagerConfig

	configsAPI configs_client.Client

	// The fallback config is stored as a string and parsed every time it's needed
	// because we mutate the parsed results and don't want those changes to take
	// effect here.
	fallbackConfig string

	// All the organization configurations that we have. Only used for instrumentation.
	cfgs map[string]configs.Config

	alertmanagersMtx sync.Mutex
	alertmanagers    map[string]*Alertmanager

	latestConfig configs.ID
	latestMutex  sync.RWMutex

	userRegistriesMtx sync.Mutex
	userRegistries    map[string]prometheus.Registerer

	peer *cluster.Peer

	stop chan struct{}
	done chan struct{}
}

// NewMultitenantAlertmanager creates a new MultitenantAlertmanager.
func NewMultitenantAlertmanager(cfg *MultitenantAlertmanagerConfig, cfgCfg configs_client.Config) (*MultitenantAlertmanager, error) {
	err := os.MkdirAll(cfg.DataDir, 0777)
	if err != nil {
		return nil, fmt.Errorf("unable to create Alertmanager data directory %q: %s", cfg.DataDir, err)
	}

	configsAPI, err := configs_client.New(cfgCfg)
	if err != nil {
		return nil, err
	}

	var fallbackConfig []byte
	if cfg.FallbackConfigFile != "" {
		fallbackConfig, err = ioutil.ReadFile(cfg.FallbackConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read fallback config %q: %s", cfg.FallbackConfigFile, err)
		}
		_, err = amconfig.LoadFile(cfg.FallbackConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to load fallback config %q: %s", cfg.FallbackConfigFile, err)
		}
	}

	var peer *cluster.Peer
	if cfg.clusterBindAddr != "" {
		peer, err = cluster.Create(
			log.With(util.Logger, "component", "cluster"),
			prometheus.DefaultRegisterer,
			cfg.clusterBindAddr,
			cfg.clusterAdvertiseAddr,
			cfg.peers,
			true,
			cluster.DefaultPushPullInterval,
			cluster.DefaultGossipInterval,
			cluster.DefaultTcpTimeout,
			cluster.DefaultProbeTimeout,
			cluster.DefaultProbeInterval,
		)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to initialize gossip mesh", "err", err)
			os.Exit(1)
		}
		err = peer.Join(cluster.DefaultReconnectInterval, cluster.DefaultReconnectTimeout)
		if err != nil {
			level.Warn(util.Logger).Log("msg", "unable to join gossip mesh", "err", err)
		}
		go peer.Settle(context.Background(), cluster.DefaultGossipInterval)
	}

	am := &MultitenantAlertmanager{
		cfg:            cfg,
		configsAPI:     configsAPI,
		fallbackConfig: string(fallbackConfig),
		cfgs:           map[string]configs.Config{},
		alertmanagers:  map[string]*Alertmanager{},
		userRegistries: map[string]prometheus.Registerer{},
		peer:           peer,
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
	}
	return am, nil
}

// Run the MultitenantAlertmanager.
func (am *MultitenantAlertmanager) Run() {
	defer close(am.done)

	// Load initial set of all configurations before polling for new ones.
	am.addNewConfigs(am.loadAllConfigs())
	ticker := time.NewTicker(am.cfg.PollInterval)
	for {
		select {
		case now := <-ticker.C:
			err := am.updateConfigs(now)
			if err != nil {
				level.Warn(util.Logger).Log("msg", "MultitenantAlertmanager: error updating configs", "err", err)
			}
		case <-am.stop:
			ticker.Stop()
			return
		}
	}
}

// Stop stops the MultitenantAlertmanager.
func (am *MultitenantAlertmanager) Stop() {
	close(am.stop)
	<-am.done
	am.alertmanagersMtx.Lock()
	for _, am := range am.alertmanagers {
		am.Stop()
	}
	am.alertmanagersMtx.Unlock()
	am.peer.Leave(am.cfg.peerTimeout)
	level.Debug(util.Logger).Log("msg", "MultitenantAlertmanager stopped")
}

// Load the full set of configurations from the server, retrying with backoff
// until we can get them.
func (am *MultitenantAlertmanager) loadAllConfigs() map[string]configs.View {
	backoff := util.NewBackoff(context.Background(), backoffConfig)
	for {
		cfgs, err := am.poll()
		if err == nil {
			level.Debug(util.Logger).Log("msg", "MultitenantAlertmanager: initial configuration load", "num_configs", len(cfgs))
			return cfgs
		}
		level.Warn(util.Logger).Log("msg", "MultitenantAlertmanager: error fetching all configurations, backing off", "err", err)
		backoff.Wait()
	}
}

func (am *MultitenantAlertmanager) updateConfigs(now time.Time) error {
	cfgs, err := am.poll()
	if err != nil {
		return err
	}
	am.addNewConfigs(cfgs)
	return nil
}

// poll the configuration server. Not re-entrant.
func (am *MultitenantAlertmanager) poll() (map[string]configs.View, error) {
	configID := am.latestConfig
	cfgs, err := am.configsAPI.GetAlerts(context.Background(), configID)
	if err != nil {
		level.Warn(util.Logger).Log("msg", "MultitenantAlertmanager: configs server poll failed", "err", err)
		return nil, err
	}
	am.latestMutex.Lock()
	am.latestConfig = cfgs.GetLatestConfigID()
	am.latestMutex.Unlock()
	return cfgs.Configs, nil
}

func (am *MultitenantAlertmanager) addNewConfigs(cfgs map[string]configs.View) {
	// TODO: instrument how many configs we have, both valid & invalid.
	level.Debug(util.Logger).Log("msg", "adding configurations", "num_configs", len(cfgs))
	for userID, config := range cfgs {
		if config.IsDeleted() {
			am.deleteUser(userID)
			continue
		}
		err := am.setConfig(userID, config.Config)
		if err != nil {
			level.Warn(util.Logger).Log("msg", "MultitenantAlertmanager: error applying config", "err", err)
			continue
		}

	}
	totalConfigs.Set(float64(len(am.cfgs)))
}

func (am *MultitenantAlertmanager) transformConfig(userID string, amConfig *amconfig.Config) (*amconfig.Config, error) {
	if amConfig == nil { // shouldn't happen, but check just in case
		return nil, fmt.Errorf("no usable Cortex configuration for %v", userID)
	}
	if am.cfg.AutoWebhookRoot != "" {
		for _, r := range amConfig.Receivers {
			for _, w := range r.WebhookConfigs {
				if w.URL.String() == autoWebhookURL {
					u, err := url.Parse(am.cfg.AutoWebhookRoot + "/" + userID + "/monitor")
					if err != nil {
						return nil, err
					}
					w.URL = &amconfig.URL{URL: u}
				}
			}
		}
	}

	return amConfig, nil
}

func (am *MultitenantAlertmanager) createTemplatesFile(userID, fn, content string) (bool, error) {
	dir := filepath.Join(am.cfg.DataDir, "templates", userID, filepath.Dir(fn))
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return false, fmt.Errorf("unable to create Alertmanager templates directory %q: %s", dir, err)
	}

	file := filepath.Join(dir, fn)
	// Check if the template file already exists and if it has changed
	if tmpl, err := ioutil.ReadFile(file); err == nil && string(tmpl) == content {
		return false, nil
	}

	if err := ioutil.WriteFile(file, []byte(content), 0644); err != nil {
		return false, fmt.Errorf("unable to create Alertmanager template file %q: %s", file, err)
	}

	return true, nil
}

// setConfig applies the given configuration to the alertmanager for `userID`,
// creating an alertmanager if it doesn't already exist.
func (am *MultitenantAlertmanager) setConfig(userID string, config configs.Config) error {
	am.alertmanagersMtx.Lock()
	existing, hasExisting := am.alertmanagers[userID]
	am.alertmanagersMtx.Unlock()
	var amConfig *amconfig.Config
	var err error
	var hasTemplateChanges bool

	for fn, content := range config.TemplateFiles {
		hasChanged, err := am.createTemplatesFile(userID, fn, content)
		if err != nil {
			return err
		}

		if hasChanged {
			hasTemplateChanges = true
		}
	}

	if config.AlertmanagerConfig == "" {
		if am.fallbackConfig == "" {
			return fmt.Errorf("blank Alertmanager configuration for %v", userID)
		}
		level.Info(util.Logger).Log("msg", "blank Alertmanager configuration; using fallback", "user_id", userID)
		amConfig, err = amconfig.Load(am.fallbackConfig)
		if err != nil {
			return fmt.Errorf("unable to load fallback configuration for %v: %v", userID, err)
		}
	} else {
		amConfig, err = alertmanagerConfigFromConfig(config)
		if err != nil && hasExisting {
			// XXX: This means that if a user has a working configuration and
			// they submit a broken one, we'll keep processing the last known
			// working configuration, and they'll never know.
			// TODO: Provide a way of communicating this to the user and for removing
			// Alertmanager instances.
			return fmt.Errorf("invalid Cortex configuration for %v: %v", userID, err)
		}
	}

	if amConfig, err = am.transformConfig(userID, amConfig); err != nil {
		return err
	}

	// If no Alertmanager instance exists for this user yet, start one.
	if !hasExisting {
		newAM, err := am.newAlertmanager(userID, amConfig)
		if err != nil {
			return err
		}
		am.alertmanagersMtx.Lock()
		am.alertmanagers[userID] = newAM
		am.alertmanagersMtx.Unlock()
	} else if am.cfgs[userID].AlertmanagerConfig != config.AlertmanagerConfig || hasTemplateChanges {
		// If the config changed, apply the new one.
		err := existing.ApplyConfig(userID, amConfig)
		if err != nil {
			return fmt.Errorf("unable to apply Alertmanager config for user %v: %v", userID, err)
		}
	}
	am.cfgs[userID] = config
	return nil
}

// alertmanagerConfigFromConfig returns the Alertmanager config from the Cortex configuration.
func alertmanagerConfigFromConfig(c configs.Config) (*amconfig.Config, error) {
	cfg, err := amconfig.Load(c.AlertmanagerConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing Alertmanager config: %s", err)
	}
	return cfg, nil
}

func (am *MultitenantAlertmanager) deleteUser(userID string) {
	am.alertmanagersMtx.Lock()
	if existing, hasExisting := am.alertmanagers[userID]; hasExisting {
		existing.Stop()
	}
	delete(am.alertmanagers, userID)
	delete(am.cfgs, userID)
	am.alertmanagersMtx.Unlock()
}

func (am *MultitenantAlertmanager) newAlertmanager(userID string, amConfig *amconfig.Config) (*Alertmanager, error) {
	am.userRegistriesMtx.Lock()
	userRegistry, exists := am.userRegistries[userID]
	if !exists {
		userRegistry = prometheus.WrapRegistererWith(prometheus.Labels{"user": userID}, prometheus.DefaultRegisterer)
		am.userRegistries[userID] = userRegistry
	}
	am.userRegistriesMtx.Unlock()

	newAM, err := New(&Config{
		UserID:      userID,
		DataDir:     am.cfg.DataDir,
		Logger:      util.Logger,
		Peer:        am.peer,
		PeerTimeout: am.cfg.peerTimeout,
		Retention:   am.cfg.Retention,
		ExternalURL: am.cfg.ExternalURL.URL,
		Registry:    userRegistry,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to start Alertmanager for user %v: %v", userID, err)
	}

	if err := newAM.ApplyConfig(userID, amConfig); err != nil {
		return nil, fmt.Errorf("unable to apply initial config for user %v: %v", userID, err)
	}
	return newAM, nil
}

// ServeHTTP serves the Alertmanager's web UI and API.
func (am *MultitenantAlertmanager) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	userID, _, err := user.ExtractOrgIDFromHTTPRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	am.alertmanagersMtx.Lock()
	userAM, ok := am.alertmanagers[userID]
	am.alertmanagersMtx.Unlock()
	if !ok {
		http.Error(w, fmt.Sprintf("no Alertmanager for this user ID"), http.StatusNotFound)
		return
	}
	userAM.router.ServeHTTP(w, req)
}

// GetStatusHandler returns the status handler for this multi-tenant
// alertmanager.
func (am *MultitenantAlertmanager) GetStatusHandler() StatusHandler {
	return StatusHandler{
		am: am,
	}
}

// StatusHandler shows the status of the alertmanager.
type StatusHandler struct {
	am *MultitenantAlertmanager
}

// ServeHTTP serves the status of the alertmanager.
func (s StatusHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	err := statusTemplate.Execute(w, s.am.peer.Info())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
