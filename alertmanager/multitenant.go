package alertmanager

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"

	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/user"
	configs "github.com/weaveworks/cortex/configs/client"
	"github.com/weaveworks/cortex/util"
	"github.com/weaveworks/mesh"
)

const (
	// Backoff for loading initial configuration set.
	minBackoff = 100 * time.Millisecond
	maxBackoff = 2 * time.Second
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
)

func init() {
	prometheus.MustRegister(configsRequestDuration)
	prometheus.MustRegister(totalConfigs)
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
	MeshPeers      stringset
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *MultitenantAlertmanagerConfig) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.DataDir, "alertmanager.storage.path", "data/", "Base path for data storage.")
	flag.DurationVar(&cfg.Retention, "alertmanager.storage.retention", 5*24*time.Hour, "How long to keep data for.")

	flag.Var(&cfg.ExternalURL, "alertmanager.web.external-url", "The URL under which Alertmanager is externally reachable (for example, if Alertmanager is served via a reverse proxy). Used for generating relative and absolute links back to Alertmanager itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Alertmanager. If omitted, relevant URL components will be derived automatically.")

	flag.Var(&cfg.ConfigsAPIURL, "alertmanager.configs.url", "URL of configs API server.")
	flag.DurationVar(&cfg.PollInterval, "alertmanager.configs.poll-interval", 15*time.Second, "How frequently to poll Cortex configs")
	flag.DurationVar(&cfg.ClientTimeout, "alertmanager.configs.client-timeout", 5*time.Second, "Timeout for requests to Weave Cloud configs service.")

	flag.StringVar(&cfg.MeshListenAddr, "alertmanager.mesh.listen-address", net.JoinHostPort("0.0.0.0", strconv.Itoa(mesh.Port)), "Mesh listen address")
	flag.StringVar(&cfg.MeshHWAddr, "alertmanager.mesh.hardware-address", mustHardwareAddr(), "MAC address, i.e. Mesh peer ID")
	flag.StringVar(&cfg.MeshNickname, "alertmanager.mesh.nickname", mustHostname(), "Mesh peer nickname")
	flag.StringVar(&cfg.MeshPassword, "alertmanager.mesh.password", "", "Password to join the Mesh peer network (empty password disables encryption)")
	flag.Var(&cfg.MeshPeers, "alertmanager.mesh.peer", "Initial Mesh peers (may be repeated)")
}

// A MultitenantAlertmanager manages Alertmanager instances for multiple
// organizations.
type MultitenantAlertmanager struct {
	cfg *MultitenantAlertmanagerConfig

	configsAPI configs.AlertManagerConfigsAPI

	// All the organization configurations that we have. Only used for instrumentation.
	cfgs map[string]configs.CortexConfig

	alertmanagersMtx sync.Mutex
	alertmanagers    map[string]*Alertmanager

	latestConfig configs.ConfigID
	latestMutex  sync.RWMutex

	meshRouter *mesh.Router

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
	defer mrouter.Stop()

	mrouter.ConnectionMaker.InitiateConnections(cfg.MeshPeers.slice(), true)

	configsAPI := configs.AlertManagerConfigsAPI{
		URL:     cfg.ConfigsAPIURL.URL,
		Timeout: cfg.ClientTimeout,
	}

	return &MultitenantAlertmanager{
		cfg:           cfg,
		configsAPI:    configsAPI,
		cfgs:          map[string]configs.CortexConfig{},
		alertmanagers: map[string]*Alertmanager{},
		meshRouter:    mrouter,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}, nil
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
	close(am.stop)
	<-am.done
	for _, am := range am.alertmanagers {
		am.Stop()
	}
	log.Debugf("MultitenantAlertmanager stopped")
}

// Load the full set of configurations from the server, retrying with backoff
// until we can get them.
func (am *MultitenantAlertmanager) loadAllConfigs() map[string]configs.CortexConfigView {
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
func (am *MultitenantAlertmanager) poll() (map[string]configs.CortexConfigView, error) {
	configID := am.latestConfig
	var cfgs *configs.CortexConfigsResponse
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

func (am *MultitenantAlertmanager) addNewConfigs(cfgs map[string]configs.CortexConfigView) {
	// TODO: instrument how many configs we have, both valid & invalid.
	log.Debugf("Adding %d configurations", len(cfgs))
	for userID, config := range cfgs {
		amConfig, err := config.Config.GetAlertmanagerConfig()
		if err != nil {
			// XXX: This means that if a user has a working configuration and
			// they submit a broken one, we'll keep processing the last known
			// working configuration, and they'll never know.
			// TODO: Provide a way of communicating this to the user and for removing
			// Alertmanager instances.
			log.Warnf("MultitenantAlertmanager: invalid Cortex configuration for %v: %v", userID, err)
			continue
		}

		// If no Alertmanager instance exists for this user yet, start one.
		if _, ok := am.alertmanagers[userID]; !ok {
			newAM, err := New(&Config{
				UserID:      userID,
				DataDir:     am.cfg.DataDir,
				Logger:      log.NewLogger(os.Stderr),
				MeshRouter:  am.meshRouter,
				Retention:   am.cfg.Retention,
				ExternalURL: am.cfg.ExternalURL.URL,
			})
			if err != nil {
				log.Warnf("MultitenantAlertmanager: unable to start Alertmanager for user %v: %v", userID, err)
				continue
			}

			if err := newAM.ApplyConfig(amConfig); err != nil {
				log.Warnf("MultitenantAlertmanager: unable to apply initial config for user %v: %v", userID, err)
				continue
			}

			am.alertmanagersMtx.Lock()
			am.alertmanagers[userID] = newAM
			am.alertmanagersMtx.Unlock()
		} else if am.cfgs[userID].AlertmanagerConfig != config.Config.AlertmanagerConfig {
			// If the config changed, apply the new one.
			err := am.alertmanagers[userID].ApplyConfig(amConfig)
			if err != nil {
				log.Warnf("MultitenantAlertmanager: unable to apply Alertmanager config for user %v: %v", userID, err)
			}
		}
		am.cfgs[userID] = config.Config
	}
	totalConfigs.Set(float64(len(am.cfgs)))
}

// ServeHTTP serves the Alertmanager's web UI and API.
func (am *MultitenantAlertmanager) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	userID, _, err := user.ExtractFromHTTPRequest(req)
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
