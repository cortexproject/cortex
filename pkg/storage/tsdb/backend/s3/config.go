package s3

import "flag"

type Config struct {
	Endpoint        string `yaml:"endpoint"`
	BucketName      string `yaml:"bucket_name"`
	SecretAccessKey string `yaml:"secret_access_key"`
	AccessKeyID     string `yaml:"access_key_id"`
	Insecure        bool   `yaml:"insecure"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.AccessKeyID, "tsdb.s3.access-key-id", "", "S3 access key ID")
	f.StringVar(&cfg.SecretAccessKey, "tsdb.s3.secret-access-key", "", "S3 secret access key")
	f.StringVar(&cfg.BucketName, "tsdb.s3.bucket-name", "", "S3 bucket name")
	f.StringVar(&cfg.Endpoint, "tsdb.s3.endpoint", "", "S3 endpoint without schema")
	f.BoolVar(&cfg.Insecure, "tsdb.s3.insecure", false, "If enabled, use http:// for the S3 endpoint instead of https://")
}
