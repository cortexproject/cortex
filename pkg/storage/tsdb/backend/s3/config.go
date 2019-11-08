package s3

import (
	"flag"
	"strings"
)

// Config holds the config options for an S3 backend
type Config struct {
	Endpoint        string `yaml:"endpoint"`
	BucketName      string `yaml:"bucket_name"`
	SecretAccessKey string `yaml:"secret_access_key"`
	AccessKeyID     string `yaml:"access_key_id"`
	Insecure        bool   `yaml:"insecure"`
}

// RegisterFlags registers the flags for TSDB s3 storage
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.AccessKeyID, "experimental.tsdb.s3.access-key-id", "", "S3 access key ID")
	f.StringVar(&cfg.SecretAccessKey, "experimental.tsdb.s3.secret-access-key", "", "S3 secret access key")
	f.StringVar(&cfg.BucketName, "experimental.tsdb.s3.bucket-name", "", "S3 bucket name")
	f.StringVar(&cfg.Endpoint, "experimental.tsdb.s3.endpoint", "", "S3 endpoint without schema")
	f.BoolVar(&cfg.Insecure, "experimental.tsdb.s3.insecure", false, "If enabled, use http:// for the S3 endpoint instead of https://")
}

// MultiBucketConfig holds the config options for a multiple bucket s3 backend
type MultiBucketConfig struct {
	Endpoints       EndpointFlags `yaml:"endpoints"`
	SecretAccessKey string        `yaml:"secret_access_key"`
	AccessKeyID     string        `yaml:"access_key_id"`
	Insecure        bool          `yaml:"insecure"`
}

// RegisterFlags registers the flags for TSDB s3 multi-bucket storage
func (cfg *MultiBucketConfig) RegisterFlags(f *flag.FlagSet) {
	f.Var(&cfg.Endpoints, "experimental.tsdb.s3.endpoint-from", "endpoint names with a bucket prefix and a date to start using the endpoint from (ex: bucket.endpoint|2019-01-01)")
	f.StringVar(&cfg.AccessKeyID, "experimental.tsdb.s3.access-key-id", "", "S3 access key ID")
	f.StringVar(&cfg.SecretAccessKey, "experimental.tsdb.s3.secret-access-key", "", "S3 secret access key")
	f.StringVar(&cfg.BucketName, "experimental.tsdb.s3.bucket-name", "", "S3 bucket name")
	f.BoolVar(&cfg.Insecure, "experimental.tsdb.s3.insecure", false, "If enabled, use http:// for the S3 endpoint instead of https://")
}

// EndpointFlags is a list of flags in the format <s3_endpoint>|<date>
type EndpointFlags []string

func (e *EndpointFlags) String() string {
	return "list of endpoint names with a bucket prefix and a date to start using the endpoint from"
}

// Set does something
func (e *EndpointFlags) Set(value string) error {
	// flag.Parse gets called twice, dedupe
	for _, s := range *e {
		if s == value {
			return nil
		}
	}

	*e = append(*e, value)
	return nil
}

// BucketName returns the bucket name from a endpoint flag string
func BucketName(s string) string {
	return strings.Split(s, ".")[0]
}

// Endpoint returns the S3 endpoint from a endpoint flag string
func Endpoint(s string) string {
	return strings.TrimPrefix(strings.Split(s, "|")[0], BucketName(s)+".")
}

// From returns the from date to start using the bucet endpoint
func From(s string) string {
	b := strings.Split(s, "|")
	return b[len(b)-1]
}
