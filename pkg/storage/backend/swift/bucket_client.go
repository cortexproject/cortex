package swift

import (
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/swift"
	"gopkg.in/yaml.v2"
)

// NewBucketClient creates a new S3 bucket client
func NewBucketClient(cfg Config, name string, logger log.Logger) (objstore.Bucket, error) {
	conf, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal swift config into yaml")
	}
	return swift.NewContainer(logger, conf)
}
