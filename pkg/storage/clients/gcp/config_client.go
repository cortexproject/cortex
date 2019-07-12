package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/ruler/group"
	"github.com/cortexproject/cortex/pkg/util"

	gstorage "cloud.google.com/go/storage"
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
	errBadRuleGroup = errors.New("unable to decompose handle for rule object")
)

// GCSConfig is config for the GCS Chunk Client.
type GCSConfig struct {
	BucketName string `yaml:"bucket_name"`
}

// RegisterFlagsWithPrefix registers flags.
func (cfg *GCSConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, prefix+"gcs.bucketname", "", "Name of GCS bucket to put chunks in.")
}

// GCSClient acts as a config backend. It is not safe to use concurrently when polling for rules.
// This is not an issue with the current scheduler architecture, but must be noted.
type GCSClient struct {
	client *gstorage.Client
	bucket *gstorage.BucketHandle

	alertPolled time.Time
	rulePolled  time.Time
}

// NewGCSClient makes a new chunk.ObjectClient that writes chunks to GCS.
func NewGCSClient(ctx context.Context, cfg GCSConfig) (*GCSClient, error) {
	client, err := gstorage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return newGCSClient(cfg, client), nil
}

// newGCSClient makes a new chunk.ObjectClient that writes chunks to GCS.
func newGCSClient(cfg GCSConfig, client *gstorage.Client) *GCSClient {
	bucket := client.Bucket(cfg.BucketName)
	return &GCSClient{
		client: client,
		bucket: bucket,

		alertPolled: time.Unix(0, 0),
		rulePolled:  time.Unix(0, 0),
	}
}

func (g *GCSClient) getAlertConfig(ctx context.Context, obj string) (alertmanager.AlertConfig, error) {
	reader, err := g.bucket.Object(obj).NewReader(ctx)
	if err == gstorage.ErrObjectNotExist {
		level.Debug(util.Logger).Log("msg", "object does not exist", "name", obj)
		return alertmanager.AlertConfig{}, nil
	}
	if err != nil {
		return alertmanager.AlertConfig{}, err
	}
	defer reader.Close()

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return alertmanager.AlertConfig{}, err
	}

	config := alertmanager.AlertConfig{}
	err = json.Unmarshal(buf, &config)
	if err != nil {
		return alertmanager.AlertConfig{}, err
	}

	return config, nil
}

// PollAlertConfigs returns any recently updated alert configurations
func (g *GCSClient) PollAlertConfigs(ctx context.Context) (map[string]alertmanager.AlertConfig, error) {
	objs := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: alertPrefix,
	})

	alertMap := map[string]alertmanager.AlertConfig{}
	for {
		objAttrs, err := objs.Next()
		if err == iterator.Done {
			break
		}
		level.Debug(util.Logger).Log("msg", "checking gcs config", "config", objAttrs.Name)

		if err != nil {
			return nil, err
		}

		if objAttrs.Updated.After(g.alertPolled) {
			level.Debug(util.Logger).Log("msg", "adding updated gcs config", "config", objAttrs.Name)
			rls, err := g.getAlertConfig(ctx, objAttrs.Name)
			if err != nil {
				return nil, err
			}
			alertMap[strings.TrimPrefix(objAttrs.Name, alertPrefix)] = rls
		}
	}

	g.alertPolled = time.Now()
	return alertMap, nil
}

// AlertStore returns an AlertStore from the client
func (g *GCSClient) AlertStore() alertmanager.AlertStore {
	return g
}

// GetAlertConfig returns a specified users alertmanager configuration
func (g *GCSClient) GetAlertConfig(ctx context.Context, userID string) (alertmanager.AlertConfig, error) {
	return g.getAlertConfig(ctx, alertPrefix+userID)
}

// SetAlertConfig sets a specified users alertmanager configuration
func (g *GCSClient) SetAlertConfig(ctx context.Context, userID string, cfg alertmanager.AlertConfig) error {
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

// DeleteAlertConfig deletes a specified users alertmanager configuration
func (g *GCSClient) DeleteAlertConfig(ctx context.Context, userID string) error {
	err := g.bucket.Object(alertPrefix + userID).Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

// PollRules returns a users recently updated rule set
func (g *GCSClient) PollRules(ctx context.Context) (map[string][]ruler.RuleGroup, error) {
	objs := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: rulePrefix,
	})

	updatedUsers := []string{}
	for {
		handle, err := objs.Next()
		if err == iterator.Done {
			break
		}
		user, _, _, err := decomposeRuleHande(handle.Name)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to poll user for rules", "user", user)
		}

		level.Debug(util.Logger).Log("msg", "checking gcs for updated rules", "user", user)

		if err != nil {
			return nil, err
		}

		updated, err := g.checkUser(ctx, user)
		if err != nil {
			return nil, err
		}

		if updated {
			level.Info(util.Logger).Log("msg", "updated rules found", "user", user)
			updatedUsers = append(updatedUsers, user)
		}
	}

	ruleMap := map[string][]ruler.RuleGroup{}

	for _, user := range updatedUsers {
		rgs, err := g.getAllRuleGroups(ctx, user)
		if err != nil {
			return nil, err
		}

		ruleMap[user] = rgs
	}

	g.rulePolled = time.Now()
	return ruleMap, nil
}

// RuleStore returns an RuleStore from the client
func (g *GCSClient) RuleStore() ruler.RuleStore {
	return g
}

func (g *GCSClient) checkUser(ctx context.Context, userID string) (bool, error) {
	objs := g.bucket.Objects(ctx, &gstorage.Query{
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

		if rg.Updated.After(g.rulePolled) {
			level.Debug(util.Logger).Log("msg", "updated rulegroups found", "user", userID)
			return true, nil
		}
	}

	return false, nil
}

func (g *GCSClient) getAllRuleGroups(ctx context.Context, userID string) ([]ruler.RuleGroup, error) {
	it := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: generateRuleHandle(userID, "", ""),
	})

	rgs := []ruler.RuleGroup{}

	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return []ruler.RuleGroup{}, err
		}

		rgProto, err := g.getRuleGroup(ctx, obj.Name)
		if err != nil {
			return []ruler.RuleGroup{}, err
		}

		rg, err := group.GenerateRuleGroup(userID, rgProto)
		if err != nil {
			return []ruler.RuleGroup{}, err
		}

		rgs = append(rgs, rg)
	}

	return rgs, nil
}

func (g *GCSClient) ListRuleGroups(ctx context.Context, options ruler.RuleStoreConditions) (map[string]ruler.RuleNamespace, error) {
	it := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: generateRuleHandle(options.UserID, options.Namespace, ""),
	})

	namespaces := map[string]bool{}
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		level.Debug(util.Logger).Log("msg", "listing rule groups", "handle", obj.Name)

		_, namespace, _, err := decomposeRuleHande(obj.Name)
		if err != nil {
			return nil, err
		}
		if namespace == options.Namespace || options.Namespace == "" {
			namespaces[namespace] = true
		}
	}

	nss := map[string]ruler.RuleNamespace{}

	for namespace := range namespaces {
		level.Debug(util.Logger).Log("msg", "retrieving rule namespace", "user", options.UserID, "namespace", namespace)
		ns, err := g.getRuleNamespace(ctx, options.UserID, namespace)
		if err != nil {
			return nil, err
		}
		nss[namespace] = ns
	}

	return nss, nil
}

func (g *GCSClient) getRuleNamespace(ctx context.Context, userID string, namespace string) (ruler.RuleNamespace, error) {
	it := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: generateRuleHandle(userID, namespace, ""),
	})

	ns := ruler.RuleNamespace{
		Groups: []rulefmt.RuleGroup{},
	}

	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return ruler.RuleNamespace{}, err
		}

		rg, err := g.getRuleGroup(ctx, obj.Name)
		if err != nil {
			return ruler.RuleNamespace{}, err
		}

		ns.Groups = append(ns.Groups, *group.FromProto(rg))
	}

	return ns, nil
}

func (g *GCSClient) GetRuleGroup(ctx context.Context, userID string, namespace string, grp string) (*rulefmt.RuleGroup, error) {
	handle := generateRuleHandle(userID, namespace, grp)
	rg, err := g.getRuleGroup(ctx, handle)
	if err != nil {
		return nil, err
	}

	if rg == nil {
		return nil, ruler.ErrGroupNotFound
	}
	return group.FromProto(rg), nil
}

func (g *GCSClient) getRuleGroup(ctx context.Context, handle string) (*group.RuleGroup, error) {
	reader, err := g.bucket.Object(handle).NewReader(ctx)
	if err == gstorage.ErrObjectNotExist {
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

	rg := &group.RuleGroup{}

	err = proto.Unmarshal(buf, rg)
	if err != nil {
		return nil, err
	}

	return rg, nil
}

func (g *GCSClient) SetRuleGroup(ctx context.Context, userID string, namespace string, grp rulefmt.RuleGroup) error {
	rg := group.ToProto(namespace, grp)
	rgBytes, err := proto.Marshal(&rg)
	if err != nil {
		return err
	}

	handle := generateRuleHandle(userID, namespace, grp.Name)
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

func (g *GCSClient) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	handle := generateRuleHandle(userID, namespace, group)
	err := g.bucket.Object(handle).Delete(ctx)
	if err != nil {
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
		return "", "", "", errBadRuleGroup
	}

	// Return `user, namespace, group_name`
	return components[1], components[2], components[3], nil
}
