package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/ruler/rulegroup"

	"cloud.google.com/go/storage"
	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"google.golang.org/api/iterator"
)

const (
	alertPrefix = "alerts/"
	rulePrefix  = "rules/"
)

var (
	ErrBadRuleGroup = errors.New("unable to decompose handle for rule object")
)

// GCSConfig is config for the GCS Chunk Client.
type GCSConfig struct {
	BucketName string `yaml:"bucket_name"`
}

// RegisterFlagsWithPrefix registers flags.
func (cfg *GCSConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, prefix+"gcs.bucketname", "", "Name of GCS bucket to put chunks in.")
}

type gcsConfigClient struct {
	client *storage.Client
	bucket *storage.BucketHandle

	lastPolled time.Time
}

// NewGCSConfigClient makes a new chunk.ObjectClient that writes chunks to GCS.
func NewGCSConfigClient(ctx context.Context, cfg GCSConfig) (configs.ConfigStore, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	bucket := client.Bucket(cfg.BucketName)
	return &gcsConfigClient{
		client: client,
		bucket: bucket,

		lastPolled: time.Unix(0, 0),
	}, nil
}

func (g *gcsConfigClient) getAlertConfig(ctx context.Context, obj string) (configs.AlertConfig, error) {
	reader, err := g.bucket.Object(obj).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		level.Debug(util.Logger).Log("msg", "object does not exist", "name", obj)
		return configs.AlertConfig{}, nil
	}
	if err != nil {
		return configs.AlertConfig{}, err
	}
	defer reader.Close()

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return configs.AlertConfig{}, err
	}

	config := configs.AlertConfig{}
	err = json.Unmarshal(buf, &config)
	if err != nil {
		return configs.AlertConfig{}, err
	}

	return config, nil
}

func (g *gcsConfigClient) PollAlerts(ctx context.Context) (map[string]configs.AlertConfig, error) {
	objs := g.bucket.Objects(ctx, &storage.Query{
		Prefix: rulePrefix,
	})

	alertMap := map[string]configs.AlertConfig{}
	for {
		objAttrs, err := objs.Next()
		if err == iterator.Done {
			break
		}
		level.Debug(util.Logger).Log("msg", "checking gcs config", "config", objAttrs.Name)

		if err != nil {
			return nil, err
		}

		if objAttrs.Updated.After(g.lastPolled) {
			level.Debug(util.Logger).Log("msg", "adding updated gcs config", "config", objAttrs.Name)
			rls, err := g.getAlertConfig(ctx, objAttrs.Name)
			if err != nil {
				return nil, err
			}
			alertMap[strings.TrimPrefix(objAttrs.Name, alertPrefix)] = rls
		}
	}

	g.lastPolled = time.Now()
	return alertMap, nil
}

func (g *gcsConfigClient) GetAlertConfig(ctx context.Context, userID string) (configs.AlertConfig, error) {
	return g.getAlertConfig(ctx, alertPrefix+userID)
}

func (g *gcsConfigClient) SetAlertConfig(ctx context.Context, userID string, cfg configs.AlertConfig) error {
	cfgBytes, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	objHandle := g.bucket.Object(alertPrefix + userID)

	writer := objHandle.NewWriter(ctx)
	if _, err := writer.Write(cfgBytes); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}

func (g *gcsConfigClient) DeleteAlertConfig(ctx context.Context, userID string) error {
	return nil
}

func (g *gcsConfigClient) PollRules(ctx context.Context) (map[string][]configs.RuleGroup, error) {
	objs := g.bucket.Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    rulePrefix,
	})

	updatedUsers := []string{}
	for {
		user, err := objs.Next()
		if err == iterator.Done {
			break
		}
		level.Debug(util.Logger).Log("msg", "checking gcs for updated rules", "user", user.Name)

		if err != nil {
			return nil, err
		}

		updated, err := g.checkUser(ctx, user.Name)
		if err != nil {
			return nil, err
		}

		if updated {
			updatedUsers = append(updatedUsers, user.Name)
		}
	}

	ruleMap := map[string][]configs.RuleGroup{}

	for _, user := range updatedUsers {
		rgs, err := g.getAllRuleGroups(ctx, user)
		if err != nil {
			return nil, err
		}

		ruleMap[user] = rgs
	}

	g.lastPolled = time.Now()
	return ruleMap, nil
}

func (g *gcsConfigClient) checkUser(ctx context.Context, userID string) (bool, error) {
	objs := g.bucket.Objects(ctx, &storage.Query{
		Prefix: generateRuleHandle(userID, "", ""),
	})

	for {
		rg, err := objs.Next()
		if err == iterator.Done {
			break
		}
		level.Debug(util.Logger).Log("msg", "checking gcs config", "config", rg.Name)

		if err != nil {
			return false, err
		}

		if rg.Updated.After(g.lastPolled) {
			level.Debug(util.Logger).Log("msg", "updated rulegroups found", "user", userID)
			return true, nil
		}
	}

	return false, nil
}

func (g *gcsConfigClient) getAllRuleGroups(ctx context.Context, userID string) ([]configs.RuleGroup, error) {
	it := g.bucket.Objects(ctx, &storage.Query{
		Prefix: generateRuleHandle(userID, "", ""),
	})

	rgs := []configs.RuleGroup{}

	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return []configs.RuleGroup{}, err
		}

		rgProto, err := g.getRuleGroup(ctx, obj.Name)
		if err != nil {
			return []configs.RuleGroup{}, err
		}

		rg, err := rulegroup.GenerateRuleGroup(userID, rgProto)
		if err != nil {
			return []configs.RuleGroup{}, err
		}

		rgs = append(rgs, rg)
	}

	return rgs, nil
}

func (g *gcsConfigClient) ListRuleGroups(ctx context.Context, options configs.RuleStoreConditions) ([]configs.RuleNamespace, error) {
	it := g.bucket.Objects(ctx, &storage.Query{
		Prefix: generateRuleHandle(options.UserID, options.Namespace, ""),
	})

	namespaces := map[string]bool{}
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return []configs.RuleNamespace{}, err
		}

		level.Debug(util.Logger).Log("msg", "listing rule groups", "handle", obj.Name)

		_, namespace, _, err := decomposeRuleHande(obj.Name)
		if err != nil {
			return []configs.RuleNamespace{}, err
		}
		if obj.Name == options.Namespace && options.Namespace != "" {
			namespaces[namespace] = true
		}
	}

	nss := []configs.RuleNamespace{}

	for ns := range namespaces {
		ns, err := g.getRuleNamespace(ctx, options.UserID, ns)
		if err != nil {
			return []configs.RuleNamespace{}, err
		}
		nss = append(nss, ns)
	}

	return nss, nil
}

func (g *gcsConfigClient) getRuleNamespace(ctx context.Context, userID string, namespace string) (configs.RuleNamespace, error) {
	it := g.bucket.Objects(ctx, &storage.Query{
		Prefix: generateRuleHandle(userID, namespace, ""),
	})

	ns := configs.RuleNamespace{
		Namespace: namespace,
		Groups:    []rulefmt.RuleGroup{},
	}

	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return configs.RuleNamespace{}, err
		}

		rg, err := g.getRuleGroup(ctx, obj.Name)
		if err != nil {
			return configs.RuleNamespace{}, err
		}

		ns.Groups = append(ns.Groups, rulegroup.FromProto(rg))
	}

	return ns, nil
}

func (g *gcsConfigClient) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (rulefmt.RuleGroup, error) {
	handle := generateRuleHandle(userID, namespace, group)
	rg, err := g.getRuleGroup(ctx, handle)
	if err != nil {
		return rulefmt.RuleGroup{}, err
	}

	if rg == nil {
		return rulefmt.RuleGroup{}, configs.ErrGroupNotFound
	}
	return rulegroup.FromProto(rg), nil
}

func (g *gcsConfigClient) getRuleGroup(ctx context.Context, handle string) (*rulegroup.RuleGroup, error) {
	reader, err := g.bucket.Object(handle).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		level.Debug(util.Logger).Log("msg", "rule group does not exist", "name", handle)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	rg := &rulegroup.RuleGroup{}

	err = proto.Unmarshal(buf, rg)
	if err != nil {
		return nil, err
	}

	return rg, nil
}

func (g *gcsConfigClient) SetRuleGroup(ctx context.Context, userID string, namespace string, group rulefmt.RuleGroup) error {
	rg := rulegroup.ToProto(namespace, group)
	rgBytes, err := proto.Marshal(&rg)
	if err != nil {
		return err
	}

	handle := generateRuleHandle(userID, namespace, group.Name)
	objHandle := g.bucket.Object(handle)

	writer := objHandle.NewWriter(ctx)
	if _, err := writer.Write(rgBytes); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}

func (g *gcsConfigClient) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	handle := generateRuleHandle(userID, namespace, group)
	rg, err := g.getRuleGroup(ctx, handle)
	if err != nil {
		return nil
	}

	if rg == nil {
		return configs.ErrGroupNotFound
	}

	rg.Deleted = true

	rgBytes, err := proto.Marshal(rg)
	if err != nil {
		return err
	}

	objHandle := g.bucket.Object(handle)

	writer := objHandle.NewWriter(ctx)
	if _, err := writer.Write(rgBytes); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}

func generateRuleHandle(id, namespace, name string) string {
	prefix := rulePrefix + id + "/"
	if namespace == "" {
		return prefix
	}
	return prefix + namespace + "/" + name
}

func decomposeRuleHande(handle string) (string, string, string, error) {
	components := strings.Split(handle, "/")

	if len(components) != 4 {
		return "", "", "", ErrBadRuleGroup
	}

	// Return `user, namespace, group_name`
	return components[1], components[2], components[3], nil
}
