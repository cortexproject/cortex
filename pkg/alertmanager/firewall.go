package alertmanager

import (
	"flag"
	"fmt"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

type FirewallConfig struct {
	Block FirewallHostsSpec `yaml:"block"`
}

func (cfg *FirewallConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.Block.RegisterFlagsWithPrefix(prefix+".block", "block", f)
}

type FirewallHostsSpec struct {
	CIDRNetworks     flagext.CIDRSliceCSV `yaml:"cidr_networks"`
	PrivateAddresses bool                 `yaml:"private_addresses"`
}

func (cfg *FirewallHostsSpec) RegisterFlagsWithPrefix(prefix, action string, f *flag.FlagSet) {
	f.Var(&cfg.CIDRNetworks, prefix+".cidr_networks", fmt.Sprintf("Comma-separated list of network CIDRs to %s in Alertmanager receiver integrations.", action))
	f.BoolVar(&cfg.PrivateAddresses, prefix+".private-addresses", false, fmt.Sprintf("True to %s private and local addresses in Alertmanager receiver integrations.", action))
}
