package ruler

import (
	"fmt"
	"testing"
	"time"

	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	sd_config "github.com/prometheus/prometheus/discovery/config"
	"github.com/prometheus/prometheus/discovery/dns"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/stretchr/testify/require"
)

func TestBuildNotifierConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		ncfg *config.Config
		err  error
	}{
		{
			name: "with no valid hosts, returns an empty config",
			cfg:  &Config{},
			ncfg: &config.Config{},
		},
		{
			name: "with a single URL and no service discovery",
			cfg: &Config{
				AlertmanagerURL: "http://alertmanager.default.svc.cluster.local/alertmanager",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfig: sd_config.ServiceDiscoveryConfig{StaticConfigs: []*targetgroup.Group{{
								Targets: []model.LabelSet{{"__address__": "alertmanager.default.svc.cluster.local"}},
							}}},
						},
					},
				},
			},
		},
		{
			name: "with a single URL and service discovery",
			cfg: &Config{
				AlertmanagerURL:             "http://_http._tcp.alertmanager.default.svc.cluster.local/alertmanager",
				AlertmanagerDiscovery:       true,
				AlertmanagerRefreshInterval: time.Duration(60),
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfig: sd_config.ServiceDiscoveryConfig{DNSSDConfigs: []*dns.SDConfig{{
								Names:           []string{"_http._tcp.alertmanager.default.svc.cluster.local"},
								RefreshInterval: 60,
								Type:            "SRV",
								Port:            0,
							}}},
						},
					},
				},
			},
		},
		{
			name: "with service discovery and an invalid URL",
			cfg: &Config{
				AlertmanagerURL:       "http://_http.default.svc.cluster.local/alertmanager",
				AlertmanagerDiscovery: true,
			},
			err: fmt.Errorf("when alertmanager-discovery is on, host name must be of the form _portname._tcp.service.fqdn (is \"alertmanager.default.svc.cluster.local\")"),
		},
		{
			name: "with multiple URLs and no service discovery",
			cfg: &Config{
				AlertmanagerURL: "http://alertmanager-0.default.svc.cluster.local/alertmanager,http://alertmanager-1.default.svc.cluster.local/alertmanager",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfig: sd_config.ServiceDiscoveryConfig{StaticConfigs: []*targetgroup.Group{{
								Targets: []model.LabelSet{{"__address__": "alertmanager-0.default.svc.cluster.local"}},
							}}},
						},
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfig: sd_config.ServiceDiscoveryConfig{StaticConfigs: []*targetgroup.Group{{
								Targets: []model.LabelSet{{"__address__": "alertmanager-1.default.svc.cluster.local"}},
							}}},
						},
					},
				},
			},
		},
		{
			name: "with multiple URLs and service discovery",
			cfg: &Config{
				AlertmanagerURL:             "http://_http._tcp.alertmanager-0.default.svc.cluster.local/alertmanager,http://_http._tcp.alertmanager-1.default.svc.cluster.local/alertmanager",
				AlertmanagerDiscovery:       true,
				AlertmanagerRefreshInterval: time.Duration(60),
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfig: sd_config.ServiceDiscoveryConfig{DNSSDConfigs: []*dns.SDConfig{{
								Names:           []string{"_http._tcp.alertmanager-0.default.svc.cluster.local"},
								RefreshInterval: 60,
								Type:            "SRV",
								Port:            0,
							}}},
						},
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfig: sd_config.ServiceDiscoveryConfig{DNSSDConfigs: []*dns.SDConfig{{
								Names:           []string{"_http._tcp.alertmanager-1.default.svc.cluster.local"},
								RefreshInterval: 60,
								Type:            "SRV",
								Port:            0,
							}}},
						},
					},
				},
			},
		},
		{
			name: "with Basic Authentication",
			cfg: &Config{
				AlertmanagerURL: "http://marco:hunter2@alertmanager-0.default.svc.cluster.local/alertmanager",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							HTTPClientConfig: config_util.HTTPClientConfig{
								BasicAuth: &config_util.BasicAuth{Username: "marco", Password: "hunter2"},
							},
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfig: sd_config.ServiceDiscoveryConfig{StaticConfigs: []*targetgroup.Group{{
								Targets: []model.LabelSet{{"__address__": "alertmanager-0.default.svc.cluster.local"}},
							}}},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ncfg, err := buildNotifierConfig(tt.cfg)
			if tt.err == nil {
				require.NoError(t, err)
				require.Equal(t, tt.ncfg, ncfg)
			} else {
				require.Error(t, tt.err, err)
			}
		})
	}
}
