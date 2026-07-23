package oci

import (
	"flag"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// config options for an OCI backend
type Config struct {
	Provider             string         `yaml:"provider"`
	Bucket               string         `yaml:"bucket"`
	Compartment          string         `yaml:"compartment_ocid"`
	Tenancy              string         `yaml:"tenancy_ocid"`
	User                 string         `yaml:"user_ocid"`
	Region               string         `yaml:"region"`
	Fingerprint          string         `yaml:"fingerprint"`
	PrivateKey           flagext.Secret `yaml:"privatekey"`
	Passphrase           flagext.Secret `yaml:"passphrase"`
	PartSize             int64          `yaml:"part_size"`
	MaxRequestRetries    int            `yaml:"max_request_retries"`
	RequestRetryInterval int            `yaml:"request_retry_interval"`
}

// registers the flags for OCI storage
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// registers the flags for OCI storage with the provided prefix
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Provider, prefix+"oci.provider", "default", "The OCI configuration provider to use. Supported values are: default, instance-principal, raw, oke-workload-identity.")
	f.StringVar(&cfg.Bucket, prefix+"oci.bucket", "", "The OCI bucket name.")
	f.StringVar(&cfg.Compartment, prefix+"oci.compartment-ocid", "", "The OCID of the compartment that contains the bucket.")
	f.StringVar(&cfg.Tenancy, prefix+"oci.tenancy-ocid", "", "The OCID of the tenancy. Required when the provider is 'raw'.")
	f.StringVar(&cfg.User, prefix+"oci.user-ocid", "", "The OCID of the user. Required when the provider is 'raw'.")
	f.StringVar(&cfg.Region, prefix+"oci.region", "", "The OCI region. Required when the provider is 'raw'.")
	f.StringVar(&cfg.Fingerprint, prefix+"oci.fingerprint", "", "The fingerprint of the API signing key. Required when the provider is 'raw'.")
	f.Var(&cfg.PrivateKey, prefix+"oci.private-key", "The API signing private key in PEM format. Required when the provider is 'raw'.")
	f.Var(&cfg.Passphrase, prefix+"oci.private-key-passphrase", "The passphrase for the API signing private key, if the key is encrypted.")
	f.Int64Var(&cfg.PartSize, prefix+"oci.part-size", 0, "The part size in bytes used for multipart uploads. 0 uses the provider default.")
	f.IntVar(&cfg.MaxRequestRetries, prefix+"oci.max-request-retries", 3, "The maximum number of request attempts when encountering recoverable errors. Values of 0 or 1 disable retries.")
	f.IntVar(&cfg.RequestRetryInterval, prefix+"oci.request-retry-interval", 10, "The fixed interval in seconds to wait between request retry attempts.")
}
