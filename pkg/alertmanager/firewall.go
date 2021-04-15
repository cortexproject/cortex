package alertmanager

import (
	"flag"
	"fmt"

	"github.com/prometheus/alertmanager/config"
	commoncfg "github.com/prometheus/common/config"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	netutil "github.com/cortexproject/cortex/pkg/util/net"
)

type FirewallConfig struct {
	Block FirewallHostsSpec `yaml:"block"`
}

func (cfg *FirewallConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.Block.RegisterFlagsWithPrefix(prefix+".block", "block", f)
}

type FirewallHostsSpec struct {
	CIDRs   flagext.CIDRSliceCSV `yaml:"cidrs"`
	Private bool                 `yaml:"private_addresses"`
}

func (cfg *FirewallHostsSpec) RegisterFlagsWithPrefix(prefix, action string, f *flag.FlagSet) {
	f.Var(&cfg.CIDRs, prefix+".cidrs", fmt.Sprintf("Comma-separated list of network CIDRs to %s in Alertmanager receiver integrations.", action))
	f.BoolVar(&cfg.Private, prefix+".private-addresses", false, fmt.Sprintf("True to %s private and local addresses in Alertmanager receiver integrations.", action))
}

func injectFirewallToAlertmanagerConfig(conf *config.Config, firewallCfg FirewallConfig) *config.Config {
	firewall := netutil.NewFirewallDialer(netutil.FirewallDialerConfig{
		BlockCIDRs:   firewallCfg.Block.CIDRs,
		BlockPrivate: firewallCfg.Block.Private,
	})

	conf.Global.HTTPConfig = injectFirewallToHTTPConfig(conf.Global.HTTPConfig, firewall)

	for _, receiver := range conf.Receivers {
		for _, rc := range receiver.WebhookConfigs {
			rc.HTTPConfig = injectFirewallToHTTPConfig(rc.HTTPConfig, firewall)
		}
		for _, rc := range receiver.SlackConfigs {
			rc.HTTPConfig = injectFirewallToHTTPConfig(rc.HTTPConfig, firewall)
		}
		for _, rc := range receiver.PushoverConfigs {
			rc.HTTPConfig = injectFirewallToHTTPConfig(rc.HTTPConfig, firewall)
		}
		for _, rc := range receiver.PagerdutyConfigs {
			rc.HTTPConfig = injectFirewallToHTTPConfig(rc.HTTPConfig, firewall)
		}
		for _, rc := range receiver.OpsGenieConfigs {
			rc.HTTPConfig = injectFirewallToHTTPConfig(rc.HTTPConfig, firewall)
		}
		for _, rc := range receiver.WechatConfigs {
			rc.HTTPConfig = injectFirewallToHTTPConfig(rc.HTTPConfig, firewall)
		}
		for _, rc := range receiver.VictorOpsConfigs {
			rc.HTTPConfig = injectFirewallToHTTPConfig(rc.HTTPConfig, firewall)
		}
	}

	return conf
}

func injectFirewallToHTTPConfig(conf *commoncfg.HTTPClientConfig, firewall *netutil.FirewallDialer) *commoncfg.HTTPClientConfig {
	if conf == nil {
		return &commoncfg.HTTPClientConfig{
			DialContext: firewall.DialContext,
		}
	}

	conf.DialContext = firewall.DialContext
	return conf
}
