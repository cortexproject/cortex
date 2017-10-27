package alertmanager

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	amconfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/mesh"

	"github.com/weaveworks/cortex/pkg/configs"
	configs_client "github.com/weaveworks/cortex/pkg/configs/client"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	// Backoff for loading initial configuration set.
	minBackoff = 100 * time.Millisecond
	maxBackoff = 2 * time.Second

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
		Name:      "configs",
		Help:      "How many configs the multitenant alertmanager knows about.",
	})
	configsRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "configs_request_duration_seconds",
		Help:      "Time spent requesting configs.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "status_code"})
	totalPeers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "mesh_peers",
		Help:      "Number of peers the multitenant alertmanager knows about",
	})
	statusTemplate      *template.Template
	allConnectionStates = []string{"established", "pending", "retrying", "failed", "connecting"}
)

func init() {
	prometheus.MustRegister(configsRequestDuration)
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
	DataDir       string
	Retention     time.Duration
	ExternalURL   util.URLValue
	ConfigsAPIURL util.URLValue
	PollInterval  time.Duration
	ClientTimeout time.Duration

	MeshListenAddr string
	MeshHWAddr     string
	MeshNickname   string
	MeshPassword   string

	MeshPeerHost            string
	MeshPeerService         string
	MeshPeerRefreshInterval time.Duration

	FallbackConfigFile string
	AutoSlackRoot      string
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *MultitenantAlertmanagerConfig) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.DataDir, "alertmanager.storage.path", "data/", "Base path for data storage.")
	flag.DurationVar(&cfg.Retention, "alertmanager.storage.retention", 5*24*time.Hour, "How long to keep data for.")

	flag.Var(&cfg.ExternalURL, "alertmanager.web.external-url", "The URL under which Alertmanager is externally reachable (for example, if Alertmanager is served via a reverse proxy). Used for generating relative and absolute links back to Alertmanager itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Alertmanager. If omitted, relevant URL components will be derived automatically.")

	flag.Var(&cfg.ConfigsAPIURL, "alertmanager.configs.url", "URL of configs API server.")
	flag.StringVar(&cfg.FallbackConfigFile, "alertmanager.configs.fallback", "", "Filename of fallback config to use if none specified for instance.")
	flag.StringVar(&cfg.AutoSlackRoot, "alertmanager.configs.auto-slack-root", "", "Root of URL to generate if config is "+autoSlackURL)
	flag.DurationVar(&cfg.PollInterval, "alertmanager.configs.poll-interval", 15*time.Second, "How frequently to poll Cortex configs")
	flag.DurationVar(&cfg.ClientTimeout, "alertmanager.configs.client-timeout", 5*time.Second, "Timeout for requests to Weave Cloud configs service.")

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

	configsAPI configs_client.AlertManagerConfigsAPI

	fallbackConfig *amconfig.Config

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
func NewMultitenantAlertmanager(cfg *MultitenantAlertmanagerConfig) (*MultitenantAlertmanager, error) {
	err := os.MkdirAll(cfg.DataDir, 0777)
	if err != nil {
		return nil, fmt.Errorf("unable to create Alertmanager data directory %q: %s", cfg.DataDir, err)
	}

	mrouter := initMesh(cfg.MeshListenAddr, cfg.MeshHWAddr, cfg.MeshNickname, cfg.MeshPassword)

	mrouter.Start()

	configsAPI := configs_client.AlertManagerConfigsAPI{
		URL:     cfg.ConfigsAPIURL.URL,
		Timeout: cfg.ClientTimeout,
	}

	var fallbackConfig *amconfig.Config
	if cfg.FallbackConfigFile != "" {
		fallbackConfig, _, err = amconfig.LoadFile(cfg.FallbackConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read fallback config %q: %s", cfg.FallbackConfigFile, err)
		}
	}

	gf := newGossipFactory(mrouter)
	am := &MultitenantAlertmanager{
		cfg:            cfg,
		configsAPI:     configsAPI,
		fallbackConfig: fallbackConfig,
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
			log.Infof("Updating alertmanager peers from %v to %v", am.meshRouter.getPeers(), peers)
			errs := am.meshRouter.ConnectionMaker.InitiateConnections(peers, true)
			for _, err := range errs {
				log.Error(err)
			}
			totalPeers.Set(float64(len(peers)))
		case now := <-ticker.C:
			err := am.updateConfigs(now)
			if err != nil {
				log.Warnf("MultitenantAlertmanager: error updating configs: %v", err)
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
	log.Debugf("MultitenantAlertmanager stopped")
}

// Load the full set of configurations from the server, retrying with backoff
// until we can get them.
func (am *MultitenantAlertmanager) loadAllConfigs() map[string]configs.View {
	backoff := minBackoff
	for {
		cfgs, err := am.poll()
		if err == nil {
			log.Debugf("MultitenantAlertmanager: found %d configurations in initial load", len(cfgs))
			return cfgs
		}
		log.Warnf("MultitenantAlertmanager: error fetching all configurations, backing off: %v", err)
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
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
	var cfgs *configs_client.ConfigsResponse
	err := instrument.TimeRequestHistogram(context.Background(), "Configs.GetOrgConfigs", configsRequestDuration, func(_ context.Context) error {
		var err error
		cfgs, err = am.configsAPI.GetConfigs(configID)
		return err
	})
	if err != nil {
		log.Warnf("MultitenantAlertmanager: configs server poll failed: %v", err)
		return nil, err
	}
	am.latestMutex.Lock()
	am.latestConfig = cfgs.GetLatestConfigID()
	am.latestMutex.Unlock()
	return cfgs.Configs, nil
}

func (am *MultitenantAlertmanager) addNewConfigs(cfgs map[string]configs.View) {
	// TODO: instrument how many configs we have, both valid & invalid.
	log.Debugf("Adding %d configurations", len(cfgs))
	for userID, config := range cfgs {

		err := am.setConfig(userID, config.Config)
		if err != nil {
			log.Warnf("MultitenantAlertmanager: %v", err)
			continue
		}

	}
	totalConfigs.Set(float64(len(am.cfgs)))
}

func (am *MultitenantAlertmanager) transformConfig(userID string, amConfig *amconfig.Config) (*amconfig.Config, error) {
	if amConfig == nil { // shouldn't happen, but check just in case
		return nil, fmt.Errorf("no usable Cortex configuration for %v", userID)
	}
	newConfig, err := copyConfig(amConfig)
	if err != nil {
		log.Errorf("cannot copy config: %s", err)
		return nil, err
	}
	// Magic ability to configure a Slack receiver if config requests it
	if am.cfg.AutoSlackRoot != "" {
		for _, r := range newConfig.Receivers {
			for _, s := range r.SlackConfigs {
				if s.APIURL == autoSlackURL {
					s.APIURL = amconfig.Secret(am.cfg.AutoSlackRoot + "/" + userID + "/monitor")
				}
			}
		}
	}

	return newConfig, nil
}

// deep copy because of config struct contains a lot of references types (slices of pointers tec)
// this copy works even if we add/change other fields of config
// or if fields are changing somewhere else in the code
func copyConfig(config *amconfig.Config) (*amconfig.Config, error) {
	configBytes, err := json.Marshal(config)
	if err != nil {
		log.Errorf("cannot marshal config to json, error: %s", err)
		return nil, err
	}

	newConfig := &amconfig.Config{}

	err = json.Unmarshal(configBytes, newConfig)
	if err != nil {
		log.Errorf("cannot unmarshal json to config, error: %s", err)
		return nil, err
	}

	return newConfig, nil
}

// setConfig applies the given configuration to the alertmanager for `userID`,
// creating an alertmanager if it doesn't already exist.
func (am *MultitenantAlertmanager) setConfig(userID string, config configs.Config) error {
	_, hasExisting := am.alertmanagers[userID]
	amConfig, err := configs_client.AlertmanagerConfigFromConfig(config)
	if err != nil && (hasExisting || am.fallbackConfig == nil) {
		// XXX: This means that if a user has a working configuration and
		// they submit a broken one, we'll keep processing the last known
		// working configuration, and they'll never know.
		// TODO: Provide a way of communicating this to the user and for removing
		// Alertmanager instances.
		return fmt.Errorf("invalid Cortex configuration for %v: %v", userID, err)
	}

	if amConfig == nil && am.fallbackConfig != nil {
		log.Infof("invalid Cortex configuration; using fallback for %v", userID)
		amConfig = am.fallbackConfig
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
	} else if am.cfgs[userID].AlertmanagerConfig != config.AlertmanagerConfig {
		// If the config changed, apply the new one.
		err := am.alertmanagers[userID].ApplyConfig(amConfig)
		if err != nil {
			return fmt.Errorf("unable to apply Alertmanager config for user %v: %v", userID, err)
		}
	}
	am.cfgs[userID] = config
	return nil
}

func (am *MultitenantAlertmanager) newAlertmanager(userID string, amConfig *amconfig.Config) (*Alertmanager, error) {
	newAM, err := New(&Config{
		UserID:      userID,
		DataDir:     am.cfg.DataDir,
		Logger:      log.NewLogger(os.Stderr),
		MeshRouter:  am.meshRouter,
		Retention:   am.cfg.Retention,
		ExternalURL: am.cfg.ExternalURL.URL,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to start Alertmanager for user %v: %v", userID, err)
	}

	if err := newAM.ApplyConfig(amConfig); err != nil {
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
