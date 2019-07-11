package aws

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/rulefmt"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	awscommon "github.com/weaveworks/common/aws"
	"github.com/weaveworks/common/instrument"
)

var (
	s3RequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "s3_request_duration_seconds",
		Help:      "Time spent doing S3 requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"}))
)

func init() {
	s3RequestDuration.Register()
}

// S3ClientConfig is config for the GCS Chunk Client.
type S3ClientConfig struct {
	BucketName       string           `yaml:"bucket_name"`
	S3               flagext.URLValue `yaml:"url"`
	S3ForcePathStyle bool
}

// RegisterFlagsWithPrefix registers flags.
func (cfg *S3ClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, prefix+"s3.bucketname", "", "Name of S3 bucket to put chunks in.")
}

type s3ConfigClient struct {
	bucketName string
	S3         s3iface.S3API

	alertPolled time.Time
	rulePolled  time.Time
}

// NewS3ConfigClient makes a new S3-backed ObjectClient.
func NewS3ConfigClient(cfg S3ClientConfig) (configs.ConfigStore, error) {
	if cfg.S3.URL == nil {
		return nil, fmt.Errorf("no URL specified for S3")
	}
	s3Config, err := awscommon.ConfigFromURL(cfg.S3.URL)
	if err != nil {
		return nil, err
	}

	s3Config = s3Config.WithS3ForcePathStyle(cfg.S3ForcePathStyle) // support for Path Style S3 url if has the flag

	s3Config = s3Config.WithMaxRetries(0) // We do our own retries, so we can monitor them
	s3Client := s3.New(session.New(s3Config))
	bucketName := strings.TrimPrefix(cfg.S3.URL.Path, "/")
	client := &s3ConfigClient{
		S3:         s3Client,
		bucketName: bucketName,

		alertPolled: time.Unix(0, 0),
		rulePolled:  time.Unix(0, 0),
	}
	return client, nil
}

func (a *s3ConfigClient) PollAlertConfigs(ctx context.Context) (map[string]configs.AlertConfig, error) {
	panic("not implemented")
}

func (a *s3ConfigClient) GetAlertConfig(ctx context.Context, userID string) (configs.AlertConfig, error) {
	panic("not implemented")
}

func (a *s3ConfigClient) SetAlertConfig(ctx context.Context, userID string, config configs.AlertConfig) error {
	panic("not implemented")
}

func (a *s3ConfigClient) DeleteAlertConfig(ctx context.Context, userID string) error {
	panic("not implemented")
}

func (a *s3ConfigClient) PollRules(ctx context.Context) (map[string][]configs.RuleGroup, error) {
	panic("not implemented")
}

func (a *s3ConfigClient) ListRuleGroups(ctx context.Context, options configs.RuleStoreConditions) (map[string]configs.RuleNamespace, error) {
	panic("not implemented")
}

func (a *s3ConfigClient) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (rulefmt.RuleGroup, error) {
	panic("not implemented")
}

func (a *s3ConfigClient) SetRuleGroup(ctx context.Context, userID string, namespace string, group rulefmt.RuleGroup) error {
	panic("not implemented")
}

func (a *s3ConfigClient) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	panic("not implemented")
}
