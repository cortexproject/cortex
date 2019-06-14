package gcp

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/pkg/errors"
)

const (
	rulePrefix = "rules/"
)

type ConfigClient struct {
	cfg ConfigDBConfig

	client *storage.Client
	bucket *storage.BucketHandle

	lastPolled time.Time
}

// ConfigDBConfig is config for the GCS Chunk Client.
type ConfigDBConfig struct {
	BucketName string `yaml:"bucket_name"`
}

// RegisterFlags registers flags.
func (cfg *ConfigDBConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, "gcs.configdb.bucketname", "", "Name of GCS bucket rule and alert configurations in")
}

// NewConfigClient makes a new chunk.ObjectClient that writes chunks to GCS.
func NewConfigClient(ctx context.Context, cfg ConfigDBConfig) (*ConfigClient, error) {
	option, err := gcsInstrumentation(ctx, "configs")
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(ctx, option)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(cfg.BucketName)
	_, err = bucket.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to access bucket %v, %v", cfg.BucketName, err)
	}
	return &ConfigClient{
		cfg:        cfg,
		client:     client,
		bucket:     bucket,
		lastPolled: time.Unix(0, 0),
	}, nil
}

func (g *ConfigClient) GetRulesConfig(ctx context.Context, userID string) (configs.VersionedRulesConfig, error) {
	return g.getRulesConfig(ctx, rulePrefix+userID)
}

func (g *ConfigClient) getRulesConfig(ctx context.Context, ruleObj string) (configs.VersionedRulesConfig, error) {
	reader, err := g.bucket.Object(ruleObj).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		return configs.VersionedRulesConfig{}, nil
	}
	if err != nil {
		return configs.VersionedRulesConfig{}, err
	}
	defer reader.Close()

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return configs.VersionedRulesConfig{}, err
	}

	config := configs.VersionedRulesConfig{}
	err = json.Unmarshal(buf, &config)
	if err != nil {
		return configs.VersionedRulesConfig{}, err
	}

	config.ID = configs.ID(reader.Attrs.Generation)
	return config, nil
}

func (g *ConfigClient) SetRulesConfig(ctx context.Context, userID string, oldConfig configs.RulesConfig, newConfig configs.RulesConfig) (bool, error) {
	current, err := g.GetRulesConfig(ctx, userID)
	if err != nil {
		return false, err
	}

	// The supplied oldConfig must match the current config. If no config
	// exists, then oldConfig must be nil. Otherwise, it must exactly
	// equal the existing config.
	if oldConfig.Equal(current.Config) {
		return false, errors.New("old config provided does not match what is currently stored")
	}

	cfgBytes, err := json.Marshal(newConfig)
	if err != nil {
		return false, err
	}

	writer := g.bucket.Object(rulePrefix + userID).If(storage.Conditions{GenerationMatch: int64(current.ID)}).NewWriter(ctx)
	if _, err := writer.Write(cfgBytes); err != nil {
		return false, err
	}
	if err := writer.Close(); err != nil {
		return true, err
	}

	return true, nil
}

func (g *ConfigClient) GetAllRulesConfigs(ctx context.Context) (map[string]configs.VersionedRulesConfig, error) {
	objs := g.bucket.Objects(ctx, &storage.Query{
		Prefix: rulePrefix,
	})

	ruleMap := map[string]configs.VersionedRulesConfig{}
	for {
		objAttrs, err := objs.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		rls, err := g.getRulesConfig(ctx, objAttrs.Name)
		if err != nil {
			return nil, err
		}
		ruleMap[strings.TrimPrefix(objAttrs.Name, rulePrefix)] = rls
	}

	return ruleMap, nil
}

func (g *ConfigClient) GetRulesConfigs(ctx context.Context, since configs.ID) (map[string]configs.VersionedRulesConfig, error) {
	panic("not implemented")
}

func (g *ConfigClient) GetRules(ctx context.Context) (map[string]configs.VersionedRulesConfig, error) {
	objs := g.bucket.Objects(ctx, &storage.Query{
		Prefix: rulePrefix,
	})

	ruleMap := map[string]configs.VersionedRulesConfig{}
	for {
		objAttrs, err := objs.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		if objAttrs.Updated.After(g.lastPolled) {
			rls, err := g.getRulesConfig(ctx, objAttrs.Name)
			if err != nil {
				return nil, err
			}
			ruleMap[strings.TrimPrefix(objAttrs.Name, rulePrefix)] = rls
		}
	}

	g.lastPolled = time.Now()
	return ruleMap, nil
}

func (g *ConfigClient) GetConfig(ctx context.Context, userID string) (configs.View, error) {
	panic("not implemented")
}

func (g *ConfigClient) SetConfig(ctx context.Context, userID string, cfg configs.Config) error {
	panic("not implemented")
}

func (g *ConfigClient) GetAllConfigs(ctx context.Context) (map[string]configs.View, error) {
	panic("not implemented")
}

func (g *ConfigClient) GetConfigs(ctx context.Context, since configs.ID) (map[string]configs.View, error) {
	panic("not implemented")
}

func (g *ConfigClient) DeactivateConfig(ctx context.Context, userID string) error {
	panic("not implemented")
}

func (g *ConfigClient) RestoreConfig(ctx context.Context, userID string) error {
	panic("not implemented")
}

func (g *ConfigClient) Close() error {
	panic("not implemented")
}

func (g *ConfigClient) GetAlerts(ctx context.Context) (map[string]configs.View, error) {
	panic("not implemented")
}
