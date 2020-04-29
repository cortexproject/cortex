package swift

import (
	"flag"

	"github.com/thanos-io/thanos/pkg/objstore/swift"
)

// Config is config for the Swift Chunk Client.
type Config struct {
	swift.SwiftConfig `yaml:",inline"`
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix registers flags with prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.ContainerName, prefix+"swift.container-name", "cortex", "Name of the Swift container to put chunks in.")
	f.StringVar(&cfg.DomainName, prefix+"swift.domain-name", "", "Openstack user's domain name.")
	f.StringVar(&cfg.DomainId, prefix+"swift.domain-id", "", "Openstack user's domain id.")
	f.StringVar(&cfg.UserDomainName, prefix+"swift.user-domain-name", "", "Openstack user's domain name.")
	f.StringVar(&cfg.UserDomainID, prefix+"swift.user-domain-id", "", "Openstack user's domain id.")
	f.StringVar(&cfg.Username, prefix+"swift.username", "", "Openstack username for the api.")
	f.StringVar(&cfg.UserId, prefix+"swift.user-id", "", "Openstack userid for the api.")
	f.StringVar(&cfg.Password, prefix+"swift.password", "", "Openstack api key.")
	f.StringVar(&cfg.AuthUrl, prefix+"swift.auth-url", "", "Openstack authentication URL.")
	f.StringVar(&cfg.RegionName, prefix+"swift.region-name", "", "Openstack Region to use eg LON, ORD - default is use first region (v2,v3 auth only)")
	f.StringVar(&cfg.ProjectName, prefix+"swift.project-name", "", "Openstack project name (v2,v3 auth only).")
	f.StringVar(&cfg.ProjectID, prefix+"swift.project-id", "", "Openstack project id (v2,v3 auth only).")
	f.StringVar(&cfg.ProjectDomainName, prefix+"swift.project-domain-name", "", "Name of the project's domain (v3 auth only), only needed if it differs from the user domain.")
	f.StringVar(&cfg.ProjectDomainID, prefix+"swift.project-domain-id", "", "Id of the project's domain (v3 auth only), only needed if it differs the from user domain.")
}
