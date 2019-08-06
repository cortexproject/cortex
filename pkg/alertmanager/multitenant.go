package alertmanager

import (
	"context"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	amconfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/mesh"

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

	// If a config sets the Slack URL to this, it will be rewritten to
	// a URL derived from Config.AutoSlackRoot
	autoSlackURL = "internal://monitor"

	statusPage = `
<!doctype html>
<html>
	<head><title>Cortex Alertmanager Status</title></head>
	<body>
		<h1>Cortex Alertmanager Status</h1>
		<h2>Mesh router</h2>
		<dl>
			<dt>Protocol</dt>
			<dd>{{.Protocol}}
			{{if eq .ProtocolMinVersion .ProtocolMaxVersion}}
			{{.ProtocolMaxVersion}}
			{{else}}
			{{.ProtocolMinVersion}}..{{.ProtocolMaxVersion}}
			{{end}}
			</dd>

			<dt>Name</dt><dd>{{.Name}} ({{.NickName}})</dd>
			<dt>Encryption</dt><dd>{{state .Encryption}}</dd>
			<dt>PeerDiscovery</dt><dd>{{state .PeerDiscovery}}</dd>

			<dt>Targets</dt><dd>{{ with .Targets }}
			<ul>{{ range . }}<li>{{ . }}</li>{{ end }}</ul>
			{{ else }}No targets{{ end }}
			</dd>

			<dt>Connections</dt><dd>{{len .Connections}}{{with connectionCounts .Connections}} ({{.}}){{end}}</dd>
			<dt>Peers</dt><dd>{{len .Peers}}{{with peerConnectionCounts .Peers}} (with {{.}} connections){{end}}</dd>
			<dt>TrustedSubnets</dt><dd>{{.TrustedSubnets}}</dd>
		</dl>
		<h3>Peers</h3>
		{{ with .Peers }}
		<table>
		<tr><th>Name</th><th>NickName</th><th>UID</th><th>ShortID</th><th>Version</th><th>Established connections</th><th>Pending connections</th></tr>
		{{ range . }}
		<tr><td>{{ .Name }}</td><td>{{ .NickName }}</td><td>{{ .ShortID }}</td><td>{{ .Version }}</td><td>{{ . | establishedCount }}</td><td>{{ . | pendingCount }}</td></tr>
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
	totalPeers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "mesh_peers",
		Help:      "Number of peers the multitenant alertmanager knows about",
	})
	statusTemplate      *template.Template
	allConnectionStates = []string{"established", "pending", "retrying", "failed", "connecting"}
)

func init() {
	prometheus.MustRegister(totalConfigs)
	prometheus.MustRegister(totalPeers)
	statusTemplate = template.Must(template.New("statusPage").Funcs(map[string]interface{}{
		"state": func(enabled bool) string {
			if enabled {
				return "enabled"
			}
			return "disabled"
		},
		"connectionCounts": func(conns []mesh.LocalConnectionStatus) string {
			cs := map[string]int{}
			for _, conn := range conns {
				cs[conn.State]++
			}
			return counts(cs, allConnectionStates)
		},
		"peerConnectionCounts": func(peers []mesh.PeerStatus) string {
			cs := map[string]int{}
			for _, peer := range peers {
				for _, conn := range peer.Connections {
					if conn.Established {
						cs["established"]++
					} else {
						cs["pending"]++
					}
				}
			}
			return counts(cs, []string{"established", "pending"})
		},
		"establishedCount": func(peer mesh.PeerStatus) string {
			count := 0
			for _, conn := range peer.Connections {
				if conn.Established {
					count++
				}
			}
			return fmt.Sprintf("%d", count)
		},
		"pendingCount": func(peer mesh.PeerStatus) string {
			count := 0
			for _, conn := range peer.Connections {
				if !conn.Established {
					count++
				}
			}
			return fmt.Sprintf("%d", count)
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

	MeshListenAddr string
	MeshHWAddr     string
	MeshNickname   string
	MeshPassword   string

	MeshPeerHost            string
	MeshPeerService         string
	MeshPeerRefreshInterval time.Duration

	FallbackConfigFile string
	AutoWebhookRoot    string
	AutoSlackRoot      string
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *MultitenantAlertmanagerConfig) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.DataDir, "alertmanager.storage.path", "data/", "Base path for data storage.")
	flag.DurationVar(&cfg.Retention, "alertmanager.storage.retention", 5*24*time.Hour, "How long to keep data for.")

	flag.Var(&cfg.ExternalURL, "alertmanager.web.external-url", "The URL under which Alertmanager is externally reachable (for example, if Alertmanager is served via a reverse proxy). Used for generating relative and absolute links back to Alertmanager itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Alertmanager. If omitted, relevant URL components will be derived automatically.")

	flag.StringVar(&cfg.FallbackConfigFile, "alertmanager.configs.fallback", "", "Filename of fallback config to use if none specified for instance.")
	flag.StringVar(&cfg.AutoWebhookRoot, "alertmanager.configs.auto-webhook-root", "", "Root of URL to generate if config is "+autoWebhookURL)
	flag.StringVar(&cfg.AutoSlackRoot, "alertmanager.configs.auto-slack-root", "", "Root of URL to generate if config is "+autoSlackURL)
	flag.DurationVar(&cfg.PollInterval, "alertmanager.configs.poll-interval", 15*time.Second, "How frequently to poll Cortex configs")

	flag.StringVar(&cfg.MeshListenAddr, "alertmanager.mesh.listen-address", net.JoinHostPort("0.0.0.0", strconv.Itoa(mesh.Port)), "Mesh listen address")
	flag.StringVar(&cfg.MeshHWAddr, "alertmanager.mesh.hardware-address", mustHardwareAddr(), "MAC address, i.e. Mesh peer ID")
	flag.StringVar(&cfg.MeshNickname, "alertmanager.mesh.nickname", mustHostname(), "Mesh peer nickname")
	flag.StringVar(&cfg.MeshPassword, "alertmanager.mesh.password", "", "Password to join the Mesh peer network (empty password disables encryption)")

	flag.StringVar(&cfg.MeshPeerService, "alertmanager.mesh.peer.service", "mesh", "SRV service used to discover peers.")
	flag.StringVar(&cfg.MeshPeerHost, "alertmanager.mesh.peer.host", "", "Hostname for mesh peers.")
	flag.DurationVar(&cfg.MeshPeerRefreshInterval, "alertmanager.mesh.peer.refresh-interval", 1*time.Minute, "Period with which to poll DNS for mesh peers.")
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

	meshRouter   *gossipFactory
	srvDiscovery *srvDiscovery

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

	mrouter := initMesh(cfg.MeshListenAddr, cfg.MeshHWAddr, cfg.MeshNickname, cfg.MeshPassword)
	mrouter.Start()

	var fallbackConfig []byte
	if cfg.FallbackConfigFile != "" {
		fallbackConfig, err = ioutil.ReadFile(cfg.FallbackConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read fallback config %q: %s", cfg.FallbackConfigFile, err)
		}
		_, _, err = amconfig.LoadFile(cfg.FallbackConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to load fallback config %q: %s", cfg.FallbackConfigFile, err)
		}
	}

	gf := newGossipFactory(mrouter)
	am := &MultitenantAlertmanager{
		cfg:            cfg,
		configsAPI:     configsAPI,
		fallbackConfig: string(fallbackConfig),
		cfgs:           map[string]configs.Config{},
		alertmanagers:  map[string]*Alertmanager{},
		meshRouter:     &gf,
		srvDiscovery:   newSRVDiscovery(cfg.MeshPeerService, cfg.MeshPeerHost, cfg.MeshPeerRefreshInterval),
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
		case addrs := <-am.srvDiscovery.addresses:
			var peers []string
			for _, srv := range addrs {
				peers = append(peers, net.JoinHostPort(srv.Target, strconv.FormatUint(uint64(srv.Port), 10)))
			}
			sort.Strings(peers)
			level.Info(util.Logger).Log("msg", "updating alertmanager peers", "old", am.meshRouter.getPeers(), "new", peers)
			errs := am.meshRouter.ConnectionMaker.InitiateConnections(peers, true)
			for _, err := range errs {
				level.Error(util.Logger).Log("err", err)
			}
			totalPeers.Set(float64(len(peers)))
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
	am.srvDiscovery.Stop()
	close(am.stop)
	<-am.done
	for _, am := range am.alertmanagers {
		am.Stop()
	}
	am.meshRouter.Stop()
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
	// Magic ability to configure a Slack receiver if config requests it
	if am.cfg.AutoSlackRoot != "" {
		for _, r := range amConfig.Receivers {
			for _, s := range r.SlackConfigs {
				if s.APIURL == autoSlackURL {
					s.APIURL = amconfig.Secret(am.cfg.AutoSlackRoot + "/" + userID + "/monitor")
				}
			}
		}
	}
	if am.cfg.AutoWebhookRoot != "" {
		for _, r := range amConfig.Receivers {
			for _, w := range r.WebhookConfigs {
				if w.URL == autoWebhookURL {
					w.URL = am.cfg.AutoWebhookRoot + "/" + userID + "/monitor"
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
	_, hasExisting := am.alertmanagers[userID]
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
		err := am.alertmanagers[userID].ApplyConfig(userID, amConfig)
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
	newAM, err := New(&Config{
		UserID:      userID,
		DataDir:     am.cfg.DataDir,
		Logger:      util.Logger,
		MeshRouter:  am.meshRouter,
		Retention:   am.cfg.Retention,
		ExternalURL: am.cfg.ExternalURL.URL,
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
	meshStatus := mesh.NewStatus(s.am.meshRouter.Router)
	err := statusTemplate.Execute(w, meshStatus)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
