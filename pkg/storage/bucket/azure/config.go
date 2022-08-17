package azure

import (
	"flag"

	"github.com/cortexproject/cortex/pkg/storage/bucket/http"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// Config holds the config options for an Azure backend
type Config struct {
	StorageAccountName string         `yaml:"account_name"`
	StorageAccountKey  flagext.Secret `yaml:"account_key"`
	ContainerName      string         `yaml:"container_name"`
	Endpoint           string         `yaml:"endpoint_suffix"`
	MaxRetries         int            `yaml:"max_retries"`
	MSIResource        string         `yaml:"msi_resource"`
	UserAssignedID     string         `yaml:"user_assigned_id"`

	http.Config `yaml:"http"`
}

// RegisterFlags registers the flags for Azure storage
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix registers the flags for Azure storage
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.StorageAccountName, prefix+"azure.account-name", "", "Azure storage account name")
	f.Var(&cfg.StorageAccountKey, prefix+"azure.account-key", "Azure storage account key")
	f.StringVar(&cfg.ContainerName, prefix+"azure.container-name", "", "Azure storage container name")
	f.StringVar(&cfg.Endpoint, prefix+"azure.endpoint-suffix", "", "Azure storage endpoint suffix without schema. The account name will be prefixed to this value to create the FQDN")
	f.IntVar(&cfg.MaxRetries, prefix+"azure.max-retries", 20, "Number of retries for recoverable errors")
	f.StringVar(&cfg.MSIResource, prefix+"azure.msi-resource", "", "Azure storage MSI resource. Either this or account key must be set.")
	f.StringVar(&cfg.UserAssignedID, prefix+"azure.user-assigned-id", "", "Azure storage MSI resource managed identity client Id. If not supplied system assigned identity is used")
	cfg.Config.RegisterFlagsWithPrefix(prefix+"azure.", f)
}
