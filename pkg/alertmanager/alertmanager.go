package alertmanager

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/alertmanager/api"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/dispatch"
	"github.com/prometheus/alertmanager/inhibit"
	"github.com/prometheus/alertmanager/nflog"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/provider/mem"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/alertmanager/ui"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/mesh"
)

const notificationLogMaintenancePeriod = 15 * time.Minute

// Config configures an Alertmanager.
type Config struct {
	UserID string
	// Used to persist notification logs and silences on disk.
	DataDir     string
	Logger      log.Logger
	MeshRouter  gossipRouter
	Retention   time.Duration
	ExternalURL *url.URL
}

// An Alertmanager manages the alerts for one user.
type Alertmanager struct {
	cfg        *Config
	api        *api.API
	logger     log.Logger
	nflog      *nflog.Log
	silences   *silence.Silences
	marker     types.Marker
	alerts     *mem.Alerts
	dispatcher *dispatch.Dispatcher
	inhibitor  *inhibit.Inhibitor
	stop       chan struct{}
	wg         sync.WaitGroup
	router     *route.Router
}

// New creates a new Alertmanager.
func New(cfg *Config) (*Alertmanager, error) {
	am := &Alertmanager{
		cfg:    cfg,
		logger: log.With(cfg.Logger, "user", cfg.UserID),
		stop:   make(chan struct{}),
	}

	am.wg.Add(1)
	nflogID := fmt.Sprintf("nflog:%s", cfg.UserID)
	var err error
	am.nflog, err = nflog.New(
		nflog.WithMesh(func(g mesh.Gossiper) mesh.Gossip {
			return cfg.MeshRouter.newGossip(nflogID, g)
		}),
		nflog.WithRetention(cfg.Retention),
		nflog.WithSnapshot(filepath.Join(cfg.DataDir, nflogID)),
		nflog.WithMaintenance(notificationLogMaintenancePeriod, am.stop, am.wg.Done),
		// TODO(cortex): Build a registry that can merge metrics from multiple users.
		// For now, these metrics are ignored, as we can't register the same
		// metric twice with a single registry.
		nflog.WithMetrics(prometheus.NewRegistry()),
		nflog.WithLogger(log.With(am.logger, "component", "nflog")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create notification log: %v", err)
	}

	am.marker = types.NewMarker()

	silencesID := fmt.Sprintf("silences:%s", cfg.UserID)
	am.silences, err = silence.New(silence.Options{
		SnapshotFile: filepath.Join(cfg.DataDir, silencesID),
		Retention:    cfg.Retention,
		Logger:       log.With(am.logger, "component", "silences"),
		// TODO(cortex): Build a registry that can merge metrics from multiple users.
		// For now, these metrics are ignored, as we can't register the same
		// metric twice with a single registry.
		Metrics: prometheus.NewRegistry(),
		Gossip: func(g mesh.Gossiper) mesh.Gossip {
			return cfg.MeshRouter.newGossip(silencesID, g)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create silences: %v", err)
	}

	am.wg.Add(1)
	go func() {
		am.silences.Maintenance(15*time.Minute, filepath.Join(cfg.DataDir, silencesID), am.stop)
		am.wg.Done()
	}()

	marker := types.NewMarker()
	am.alerts, err = mem.NewAlerts(marker, 30*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to create alerts: %v", err)
	}

	am.api, err = api.New(
		am.alerts,
		am.silences,
		func(matchers []*labels.Matcher) dispatch.AlertOverview {
			return am.dispatcher.Groups(matchers)
		},
		marker.Status,
		nil, // Passing a nil mesh router since we don't show mesh router information in Cortex anyway.
		log.With(am.logger, "component", "api"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create api: %v", err)
	}

	am.router = route.New()

	webReload := make(chan chan error)
	ui.Register(am.router.WithPrefix(am.cfg.ExternalURL.Path), webReload, log.With(am.logger, "component", "ui"))
	am.api.Register(am.router.WithPrefix(path.Join(am.cfg.ExternalURL.Path, "/api")))

	go func() {
		for {
			select {
			// Since this is not a "normal" Alertmanager which reads its config
			// from disk, we just ignore web-based reload signals. Config updates are
			// only applied externally via ApplyConfig().
			case <-webReload:
			case <-am.stop:
				return
			}
		}
	}()

	return am, nil
}

// ApplyConfig applies a new configuration to an Alertmanager.
func (am *Alertmanager) ApplyConfig(userID string, conf *config.Config) error {
	var (
		tmpl     *template.Template
		pipeline notify.Stage
	)

	templateFiles := make([]string, len(conf.Templates), len(conf.Templates))
	if len(conf.Templates) > 0 {
		for i, t := range conf.Templates {
			templateFiles[i] = filepath.Join(am.cfg.DataDir, "templates", userID, t)
		}
	}

	tmpl, err := template.FromGlobs(templateFiles...)
	if err != nil {
		return err
	}
	tmpl.ExternalURL = am.cfg.ExternalURL

	err = am.api.Update(conf, time.Duration(conf.Global.ResolveTimeout))
	if err != nil {
		return err
	}

	am.inhibitor.Stop()
	am.dispatcher.Stop()

	am.inhibitor = inhibit.NewInhibitor(am.alerts, conf.InhibitRules, am.marker, log.With(am.logger, "component", "inhibitor"))

	waitFunc := meshWait(am.cfg.MeshRouter, 5*time.Second)
	timeoutFunc := func(d time.Duration) time.Duration {
		if d < notify.MinTimeout {
			d = notify.MinTimeout
		}
		return d + waitFunc()
	}

	pipeline = notify.BuildPipeline(
		conf.Receivers,
		tmpl,
		waitFunc,
		am.inhibitor,
		am.silences,
		am.nflog,
		am.marker,
		log.With(am.logger, "component", "pipeline"),
	)
	am.dispatcher = dispatch.NewDispatcher(
		am.alerts,
		dispatch.NewRoute(conf.Route, nil),
		pipeline,
		am.marker,
		timeoutFunc,
		log.With(am.logger, "component", "dispatcher"),
	)

	go am.dispatcher.Run()
	go am.inhibitor.Run()

	return nil
}

// Stop stops the Alertmanager.
func (am *Alertmanager) Stop() {
	am.dispatcher.Stop()
	am.alerts.Close()
	close(am.stop)
	am.wg.Wait()
}

// ServeHTTP serves the Alertmanager's web UI and API.
func (am *Alertmanager) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	am.router.ServeHTTP(w, req)
}
