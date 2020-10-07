package swift

import (
	"flag"
)

// Config holds the config options for GCS backend
type Config struct {
	AuthUrl           string `yaml:"auth_url"`
	Username          string `yaml:"username"`
	UserDomainName    string `yaml:"user_domain_name"`
	UserDomainID      string `yaml:"user_domain_id"`
	UserId            string `yaml:"user_id"`
	Password          string `yaml:"password"`
	DomainId          string `yaml:"domain_id"`
	DomainName        string `yaml:"domain_name"`
	ProjectID         string `yaml:"project_id"`
	ProjectName       string `yaml:"project_name"`
	ProjectDomainID   string `yaml:"project_domain_id"`
	ProjectDomainName string `yaml:"project_domain_name"`
	RegionName        string `yaml:"region_name"`
	ContainerName     string `yaml:"container_name"`
}

// RegisterFlags registers the flags for TSDB Swift storage
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("blocks-storage.", f)
}

// RegisterFlagsWithPrefix registers the flags for TSDB Swift storage with the provided prefix
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.AuthUrl, prefix+"swift.auth-url", "", "Openstack authentication URL")
	f.StringVar(&cfg.Username, prefix+"swift.username", "", "Openstack username for the api.")
	f.StringVar(&cfg.UserDomainName, prefix+"swift.user-domain-name", "", "Openstack user's domain name.")
	f.StringVar(&cfg.UserDomainID, prefix+"swift.user-domain-id", "", "Openstack user's domain id.")
	f.StringVar(&cfg.UserId, prefix+"swift.user-id", "", "Openstack userid for the api.")
	f.StringVar(&cfg.Password, prefix+"swift.password", "","Openstack api key.")
	f.StringVar(&cfg.DomainId, prefix+"swift.domain-id", "", "Openstack user's domain id.")
	f.StringVar(&cfg.DomainName, prefix+"swift.domain-name", "", "Openstack user's domain name.")
	f.StringVar(&cfg.ProjectID, prefix+"swift.project-id", "", "Openstack project id (v2,v3 auth only).")
	f.StringVar(&cfg.ProjectName, prefix+"swift.project-name", "", "Openstack project name (v2,v3 auth only)")
	f.StringVar(&cfg.ProjectDomainID, prefix+"swift.project-domain-id", "", "Id of the project's domain (v3 auth only), only needed if it differs the from user domain.")
	f.StringVar(&cfg.ProjectDomainName, prefix+"swift.project-domain-name", "", "Name of the project's domain (v3 auth only), only needed if it differs from the user domain.")
	f.StringVar(&cfg.RegionName, prefix+"swift.region-name", "", "Openstack Region to use eg LON, ORD - default is use first region (v2,v3 auth only)")
	f.StringVar(&cfg.ContainerName, prefix+"swift.container-name", "", "Name of the Swift container to put chunks in. (default \"cortex\")")
}
