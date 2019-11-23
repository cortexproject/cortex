package gcp

import (
	"context"
	"errors"
	"flag"
	"io/ioutil"
	"strings"

	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/util"

	gstorage "cloud.google.com/go/storage"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"google.golang.org/api/iterator"
)

const (
	rulePrefix = "rules/"
)

var (
	errBadRuleGroup = errors.New("unable to decompose handle for rule object")
)

// GCSRulesConfig is config for the GCS Chunk Client.
type GCSRulesConfig struct {
	BucketName string `yaml:"bucket_name"`
}

// RegisterFlags registers flags.
func (cfg *GCSRulesConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, "rules.gcs.bucketname", "", "Name of GCS bucket to put chunks in.")
}

// GCSRuleClient acts as a config backend. It is not safe to use concurrently when polling for rules.
// This is not an issue with the current scheduler architecture, but must be noted.
type GCSRuleClient struct {
	client *gstorage.Client
	bucket *gstorage.BucketHandle
}

// NewGCSRuleClient makes a new chunk.ObjectClient that writes chunks to GCS.
func NewGCSRuleClient(ctx context.Context, cfg GCSRulesConfig) (*GCSRuleClient, error) {
	client, err := gstorage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return newGCSRuleClient(cfg, client), nil
}

// newGCSRuleClient makes a new chunk.ObjectClient that writes chunks to GCS.
func newGCSRuleClient(cfg GCSRulesConfig, client *gstorage.Client) *GCSRuleClient {
	bucket := client.Bucket(cfg.BucketName)
	return &GCSRuleClient{
		client: client,
		bucket: bucket,
	}
}

func (g *GCSRuleClient) getAllRuleGroups(ctx context.Context, userID string) ([]*rules.RuleGroupDesc, error) {
	it := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: generateRuleHandle(userID, "", ""),
	})

	rgs := []*rules.RuleGroupDesc{}

	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		rgProto, err := g.getRuleGroup(ctx, obj.Name)
		if err != nil {
			return nil, err
		}

		rgs = append(rgs, rgProto)
	}

	return rgs, nil
}

// ListAllRuleGroups returns all the active rule groups
func (g *GCSRuleClient) ListAllRuleGroups(ctx context.Context) (map[string]rules.RuleGroupList, error) {
	it := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: rulePrefix,
	})

	userGroupMap := map[string]rules.RuleGroupList{}
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		user := decomposeName(obj.Name)
		if user == "" {
			continue
		}

		rg, err := g.getRuleGroup(ctx, obj.Name)
		if err != nil {
			return nil, err
		}

		if _, exists := userGroupMap[user]; !exists {
			userGroupMap[user] = rules.RuleGroupList{}
		}
		userGroupMap[user] = append(userGroupMap[user], rg)
	}

	return userGroupMap, nil
}

// ListRuleGroups returns all the active rule groups for a user
func (g *GCSRuleClient) ListRuleGroups(ctx context.Context, userID, namespace string) (rules.RuleGroupList, error) {
	it := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: generateRuleHandle(userID, namespace, ""),
	})

	groups := []*rules.RuleGroupDesc{}
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		level.Debug(util.Logger).Log("msg", "listing rule group", "handle", obj.Name)

		rg, err := g.getRuleGroup(ctx, obj.Name)
		if err != nil {
			return nil, err
		}
		groups = append(groups, rg)
	}
	return groups, nil
}

func (g *GCSRuleClient) getRuleNamespace(ctx context.Context, userID string, namespace string) ([]*rules.RuleGroupDesc, error) {
	it := g.bucket.Objects(ctx, &gstorage.Query{
		Prefix: generateRuleHandle(userID, namespace, ""),
	})

	groups := []*rules.RuleGroupDesc{}

	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		rg, err := g.getRuleGroup(ctx, obj.Name)
		if err != nil {
			return nil, err
		}

		groups = append(groups, rg)
	}

	return groups, nil
}

// GetRuleGroup returns the requested rule group
func (g *GCSRuleClient) GetRuleGroup(ctx context.Context, userID string, namespace string, grp string) (*rules.RuleGroupDesc, error) {
	handle := generateRuleHandle(userID, namespace, grp)
	rg, err := g.getRuleGroup(ctx, handle)
	if err != nil {
		return nil, err
	}

	if rg == nil {
		return nil, rules.ErrGroupNotFound
	}

	return rg, nil
}

func (g *GCSRuleClient) getRuleGroup(ctx context.Context, handle string) (*rules.RuleGroupDesc, error) {
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

	rg := &rules.RuleGroupDesc{}

	err = proto.Unmarshal(buf, rg)
	if err != nil {
		return nil, err
	}

	return rg, nil
}

// SetRuleGroup sets provided rule group
func (g *GCSRuleClient) SetRuleGroup(ctx context.Context, userID string, namespace string, grp *rules.RuleGroupDesc) error {
	rgBytes, err := proto.Marshal(grp)
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

// DeleteRuleGroup deletes the specified rule group
func (g *GCSRuleClient) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	handle := generateRuleHandle(userID, namespace, group)
	err := g.bucket.Object(handle).Delete(ctx)
	if err != nil {
		return err
	}

	return nil
}

func generateRuleHandle(id, namespace, name string) string {
	if id == "" {
		return rulePrefix
	}
	prefix := rulePrefix + id + "/"
	if namespace == "" {
		return prefix
	}
	return prefix + namespace + "/" + name
}

func decomposeName(handle string) string {
	components := strings.Split(handle, "/")
	if len(components) != 4 {
		return ""
	}
	return components[1]
}
