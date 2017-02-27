package alertmanager

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"sync"
	"time"

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
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/route"
	"github.com/weaveworks/mesh"
)

// Config configures an Alertmanager.
type Config struct {
	UserID      string
	DataDir     string
	Logger      log.Logger
	MeshRouter  *mesh.Router
	Retention   time.Duration
	ExternalURL *url.URL
}

// An Alertmanager manages the alerts for one user.
type Alertmanager struct {
	cfg        *Config
	api        *api.API
	logger     log.Logger
	nflog      nflog.Log
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
		logger: cfg.Logger.With("user", cfg.UserID),
		stop:   make(chan struct{}),
	}

	am.wg.Add(1)
	nflogID := fmt.Sprintf("nflog:%s", cfg.UserID)
	var err error
	am.nflog, err = nflog.New(
		nflog.WithMesh(func(g mesh.Gossiper) mesh.Gossip {
			return cfg.MeshRouter.NewGossip(nflogID, g)
		}),
		nflog.WithRetention(cfg.Retention),
		nflog.WithSnapshot(filepath.Join(cfg.DataDir, nflogID)),
		nflog.WithMaintenance(15*time.Minute, am.stop, am.wg.Done),
		// TODO: Build a registry that can merge metrics from multiple users.
		// For now, these metrics are ignored, as we can't register the same
		// metric twice with a single registry.
		nflog.WithMetrics(prometheus.NewRegistry()),
		nflog.WithLogger(am.logger.With("component", "nflog")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create notification log: %v", err)
	}

	am.marker = types.NewMarker()

	silencesID := fmt.Sprintf("silences:%s", cfg.UserID)
	am.silences, err = silence.New(silence.Options{
		SnapshotFile: filepath.Join(cfg.DataDir, silencesID),
		Retention:    cfg.Retention,
		Logger:       am.logger.With("component", "silences"),
		// TODO: Build a registry that can merge metrics from multiple users.
		// For now, these metrics are ignored, as we can't register the same
		// metric twice with a single registry.
		Metrics: prometheus.NewRegistry(),
		Gossip: func(g mesh.Gossiper) mesh.Gossip {
			return cfg.MeshRouter.NewGossip(silencesID, g)
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

	am.alerts, err = mem.NewAlerts(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create alerts: %v", err)
	}

	am.api = api.New(am.alerts, am.silences, func() dispatch.AlertOverview {
		return am.dispatcher.Groups()
	})

	am.router = route.New(nil)

	webReload := make(chan struct{})
	ui.Register(am.router.WithPrefix(am.cfg.ExternalURL.Path), webReload)
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
func (am *Alertmanager) ApplyConfig(conf *config.Config) error {
	var (
		tmpl     *template.Template
		pipeline notify.Stage
	)

	// TODO: How to support template files?
	if len(conf.Templates) != 0 {
		return fmt.Errorf("template files are not yet supported")
	}

	tmpl, err := template.FromGlobs()
	if err != nil {
		return err
	}
	tmpl.ExternalURL = am.cfg.ExternalURL

	err = am.api.Update(conf.String(), time.Duration(conf.Global.ResolveTimeout))
	if err != nil {
		return err
	}

	am.inhibitor.Stop()
	am.dispatcher.Stop()

	am.inhibitor = inhibit.NewInhibitor(am.alerts, conf.InhibitRules, am.marker)

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
	)
	am.dispatcher = dispatch.NewDispatcher(am.alerts, dispatch.NewRoute(conf.Route, nil), pipeline, am.marker, timeoutFunc)

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
