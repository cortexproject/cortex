package ruler

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	gklog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	sd_config "github.com/prometheus/prometheus/discovery/config"
	"github.com/prometheus/prometheus/discovery/dns"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/notifier"
)

// rulerNotifier bundles a notifier.Manager together with an associated
// Alertmanager service discovery manager and handles the lifecycle
// of both actors.
type rulerNotifier struct {
	notifier  *notifier.Manager
	sdCancel  context.CancelFunc
	sdManager *discovery.Manager
	wg        sync.WaitGroup
	logger    gklog.Logger
}

func newRulerNotifier(o *notifier.Options, l gklog.Logger) *rulerNotifier {
	sdCtx, sdCancel := context.WithCancel(context.Background())
	return &rulerNotifier{
		notifier:  notifier.NewManager(o, l),
		sdCancel:  sdCancel,
		sdManager: discovery.NewManager(sdCtx, l),
		logger:    l,
	}
}

func (rn *rulerNotifier) run() {
	rn.wg.Add(2)
	go func() {
		if err := rn.sdManager.Run(); err != nil {
			level.Error(rn.logger).Log("msg", "error starting notifier discovery manager", "err", err)
		}
		rn.wg.Done()
	}()
	go func() {
		rn.notifier.Run(rn.sdManager.SyncCh())
		rn.wg.Done()
	}()
}

func (rn *rulerNotifier) applyConfig(cfg *config.Config) error {
	if err := rn.notifier.ApplyConfig(cfg); err != nil {
		return err
	}

	sdCfgs := make(map[string]sd_config.ServiceDiscoveryConfig)
	for _, v := range cfg.AlertingConfig.AlertmanagerConfigs {
		// AlertmanagerConfigs doesn't hold an unique identifier so we use the config hash as the identifier.
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		// This hash needs to be identical to the one computed in the notifier in
		// https://github.com/prometheus/prometheus/blob/719c579f7b917b384c3d629752dea026513317dc/notifier/notifier.go#L265
		// This kind of sucks, but it's done in Prometheus in main.go in the same way.
		sdCfgs[fmt.Sprintf("%x", md5.Sum(b))] = v.ServiceDiscoveryConfig
	}
	return rn.sdManager.ApplyConfig(sdCfgs)
}

func (rn *rulerNotifier) stop() {
	rn.sdCancel()
	rn.notifier.Stop()
	rn.wg.Wait()
}

// Builds a Prometheus config.Config from a ruler.Config with just the required
// options to configure notifications to Alertmanager.
func buildNotifierConfig(rulerConfig *Config) (*config.Config, error) {
	if rulerConfig.AlertmanagerURL.URL == nil {
		return &config.Config{}, nil
	}

	u := rulerConfig.AlertmanagerURL
	var sdConfig sd_config.ServiceDiscoveryConfig
	if rulerConfig.AlertmanagerDiscovery {
		if !strings.Contains(u.Host, "_tcp.") {
			return nil, fmt.Errorf("When alertmanager-discovery is on, host name must be of the form _portname._tcp.service.fqdn (is %q)", u.Host)
		}
		dnsSDConfig := dns.SDConfig{
			Names:           []string{u.Host},
			RefreshInterval: model.Duration(rulerConfig.AlertmanagerRefreshInterval),
			Type:            "SRV",
			Port:            0, // Ignored, because of SRV.
		}
		sdConfig = sd_config.ServiceDiscoveryConfig{
			DNSSDConfigs: []*dns.SDConfig{&dnsSDConfig},
		}
	} else {
		sdConfig = sd_config.ServiceDiscoveryConfig{
			StaticConfigs: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						{
							model.AddressLabel: model.LabelValue(u.Host),
						},
					},
				},
			},
		}
	}
	amConfig := &config.AlertmanagerConfig{
		Scheme:                 u.Scheme,
		PathPrefix:             u.Path,
		Timeout:                model.Duration(rulerConfig.NotificationTimeout),
		ServiceDiscoveryConfig: sdConfig,
	}

	promConfig := &config.Config{
		AlertingConfig: config.AlertingConfig{
			AlertmanagerConfigs: []*config.AlertmanagerConfig{amConfig},
		},
	}

	if u.User != nil {
		amConfig.HTTPClientConfig = config_util.HTTPClientConfig{
			BasicAuth: &config_util.BasicAuth{
				Username: u.User.Username(),
			},
		}

		if password, isSet := u.User.Password(); isSet {
			amConfig.HTTPClientConfig.BasicAuth.Password = config_util.Secret(password)
		}
	}

	return promConfig, nil
}
