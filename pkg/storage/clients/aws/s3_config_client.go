package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/prometheus/prometheus/pkg/rulefmt"

	alertstore "github.com/cortexproject/cortex/pkg/alertmanager/storage"
	"github.com/cortexproject/cortex/pkg/ruler/store"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	awscommon "github.com/weaveworks/common/aws"
)

var (
	alertPrefix = "alerts/"
	rulePrefix  = "rules/"
)

type s3ConfigClient struct {
	bucketName string
	S3         s3iface.S3API
}

// S3Config configures the s3ConfigClient
type S3Config struct {
	S3               flagext.URLValue
	S3ForcePathStyle bool
}

// NewS3ConfigClient makes a new S3-backed ObjectClient.
func NewS3ConfigClient(cfg S3Config) (alertstore.AlertStore, error) {
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
	}

	return client, nil
}

func (s *s3ConfigClient) ListAlertConfigs(ctx context.Context) (map[string]alertstore.AlertConfig, error) {
	var continuation *string
	alerts := map[string]alertstore.AlertConfig{}

	for {
		result, err := s.listAlerts(ctx, continuation)
		if err != nil {
			return nil, err
		}

		if !*result.IsTruncated {
			break
		}

		for _, alertObj := range result.Contents {
			cfg, err := s.getAlertConfig(ctx, alertObj.Key)
			if err != nil {
				return nil, err
			}
			user := strings.TrimPrefix(*alertObj.Key, alertPrefix)
			alerts[user] = cfg
		}
	}

	return alerts, nil
}

func (s *s3ConfigClient) listAlerts(ctx context.Context, continuation *string) (*s3.ListObjectsV2Output, error) {
	return s.S3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:            &s.bucketName,
		Prefix:            &alertPrefix,
		ContinuationToken: continuation,
	})
}

func (s *s3ConfigClient) getAlertConfig(ctx context.Context, key *string) (alertstore.AlertConfig, error) {
	cfg, err := s.S3.GetObject(&s3.GetObjectInput{
		Bucket: &s.bucketName,
		Key:    key,
	})

	if err != nil {
		return alertstore.AlertConfig{}, err
	}

	defer cfg.Body.Close()

	buf, err := ioutil.ReadAll(cfg.Body)
	if err != nil {
		return alertstore.AlertConfig{}, err
	}

	config := alertstore.AlertConfig{}
	err = json.Unmarshal(buf, &config)
	if err != nil {
		return alertstore.AlertConfig{}, err
	}

	return config, nil
}

func (s *s3ConfigClient) GetAlertConfig(ctx context.Context, id string) (alertstore.AlertConfig, error) {
	panic("not implemented")
}

func (s *s3ConfigClient) SetAlertConfig(ctx context.Context, id string, cfg alertstore.AlertConfig) error {
	panic("not implemented")
}

func (s *s3ConfigClient) DeleteAlertConfig(ctx context.Context, id string) error {
	panic("not implemented")
}

func (s *s3ConfigClient) ListRuleGroups(ctx context.Context, options store.RuleStoreConditions) (store.RuleGroupList, error) {
	panic("not implemented")
}

func (s *s3ConfigClient) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (store.RuleGroup, error) {
	panic("not implemented")
}

func (s *s3ConfigClient) SetRuleGroup(ctx context.Context, userID string, namespace string, group rulefmt.RuleGroup) error {
	panic("not implemented")
}

func (s *s3ConfigClient) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	panic("not implemented")
}
