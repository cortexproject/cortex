package oss

import (
	"flag"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// Config holds the config options for an Alibaba Cloud OSS backend
type Config struct {
	Endpoint        string         `yaml:"endpoint"`
	Bucket          string         `yaml:"bucket"`
	AccessKeyID     string         `yaml:"access_key_id"`
	AccessKeySecret flagext.Secret `yaml:"access_key_secret"`
}

// RegisterFlags registers the flags for Alibaba Cloud OSS storage
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix registers the flags for Alibaba Cloud OSS storage with the provided prefix
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Endpoint, prefix+"oss.endpoint", "", "The Alibaba Cloud OSS endpoint to connect to, e.g. oss-cn-hangzhou.aliyuncs.com.")
	f.StringVar(&cfg.Bucket, prefix+"oss.bucket", "", "The Alibaba Cloud OSS bucket name.")
	f.StringVar(&cfg.AccessKeyID, prefix+"oss.access-key-id", "", "The Alibaba Cloud OSS access key ID.")
	f.Var(&cfg.AccessKeySecret, prefix+"oss.access-key-secret", "The Alibaba Cloud OSS access key secret.")
}
