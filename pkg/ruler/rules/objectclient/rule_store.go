package objectclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"io/ioutil"
	strings "strings"
	"sync"

	"github.com/go-kit/kit/log/level"
	proto "github.com/gogo/protobuf/proto"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/util"
)

// Object Rule Storage Schema
// =======================
// Object Name: "rules/<user_id>/<base64 URL Encoded: namespace>/<base64 URL Encoded: group_name>"
// Storage Format: Encoded RuleGroupDesc
//
// Prometheus Rule Groups can include a large number of characters that are not valid object names
// in common object storage systems. A URL Base64 encoding allows for generic consistent naming
// across all backends

const (
	rulePrefix = "rules/"

	loadRuleGroupsConcurrency = 4
)

// RuleStore allows cortex rules to be stored using an object store backend.
type RuleStore struct {
	client chunk.ObjectClient
}

// NewRuleStore returns a new RuleStore
func NewRuleStore(client chunk.ObjectClient) *RuleStore {
	return &RuleStore{
		client: client,
	}
}

func (o *RuleStore) getRuleGroup(ctx context.Context, objectKey string) (*rules.RuleGroupDesc, error) {
	reader, err := o.client.GetObject(ctx, objectKey)
	if err == chunk.ErrStorageObjectNotFound {
		level.Debug(util.Logger).Log("msg", "rule group does not exist", "name", objectKey)
		return nil, rules.ErrGroupNotFound
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

// ListAllRuleGroups returns all the active rule groups
func (o *RuleStore) ListAllRuleGroups(ctx context.Context) (map[string]rules.RuleGroupList, error) {
	ruleGroupObjects, _, err := o.client.List(ctx, generateRuleObjectKey("", "", ""), "")
	if err != nil {
		return nil, err
	}

	if len(ruleGroupObjects) == 0 {
		return map[string]rules.RuleGroupList{}, nil
	}

	return o.loadRuleGroupsConcurrently(ctx, ruleGroupObjects)
}

// ListRuleGroups returns all the active rule groups for a user
func (o *RuleStore) ListRuleGroups(ctx context.Context, userID, namespace string) (rules.RuleGroupList, error) {
	ruleGroupObjects, _, err := o.client.List(ctx, generateRuleObjectKey(userID, namespace, ""), "")
	if err != nil {
		return nil, err
	}

	if len(ruleGroupObjects) == 0 {
		return rules.RuleGroupList{}, nil
	}

	groups, err := o.loadRuleGroupsConcurrently(ctx, ruleGroupObjects)
	return groups[userID], err
}

// GetRuleGroup returns the requested rule group
func (o *RuleStore) GetRuleGroup(ctx context.Context, userID string, namespace string, grp string) (*rules.RuleGroupDesc, error) {
	handle := generateRuleObjectKey(userID, namespace, grp)
	rg, err := o.getRuleGroup(ctx, handle)
	if err != nil {
		return nil, err
	}

	return rg, nil
}

// SetRuleGroup sets provided rule group
func (o *RuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group *rules.RuleGroupDesc) error {
	data, err := proto.Marshal(group)
	if err != nil {
		return err
	}

	objectKey := generateRuleObjectKey(userID, namespace, group.Name)
	return o.client.PutObject(ctx, objectKey, bytes.NewReader(data))
}

// DeleteRuleGroup deletes the specified rule group
func (o *RuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, groupName string) error {
	objectKey := generateRuleObjectKey(userID, namespace, groupName)
	err := o.client.DeleteObject(ctx, objectKey)
	if err == chunk.ErrStorageObjectNotFound {
		return rules.ErrGroupNotFound
	}
	return err
}

// DeleteNamespace deletes all the rule groups in the specified namespace
func (o *RuleStore) DeleteNamespace(ctx context.Context, userID, namespace string) error {
	ruleGroupObjects, _, err := o.client.List(ctx, generateRuleObjectKey(userID, namespace, ""), "")
	if err != nil {
		return err
	}

	if len(ruleGroupObjects) == 0 {
		return rules.ErrGroupNamespaceNotFound
	}

	for _, obj := range ruleGroupObjects {
		level.Debug(util.Logger).Log("msg", "deleting rule group", "namespace", namespace, "key", obj.Key)
		err = o.client.DeleteObject(ctx, obj.Key)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to delete rule group from namespace", "err", err, "namespace", namespace, "key", obj.Key)
			return err
		}
	}

	return nil
}

func (o *RuleStore) loadRuleGroupsConcurrently(ctx context.Context, rgObjects []chunk.StorageObject) (map[string]rules.RuleGroupList, error) {
	ch := make(chan string, len(rgObjects))

	for _, obj := range rgObjects {
		ch <- obj.Key
	}
	close(ch)

	mtx := sync.Mutex{}
	result := map[string]rules.RuleGroupList{}

	concurrency := loadRuleGroupsConcurrency
	if loadRuleGroupsConcurrency < len(rgObjects) {
		concurrency = len(rgObjects)
	}

	// Given we store one file per rule group. With this, we create a pool of workers that will
	// download all rule groups in parallel. We limit the number of workers to avoid a
	// particular user having too many rule groups rate limiting us with the object storage.
	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for key := range ch {
				user := decomposeRuleObjectKey(key)
				if user == "" {
					continue
				}

				level.Debug(util.Logger).Log("msg", "listing rule group", "key", key, "user", user)
				rg, err := o.getRuleGroup(gCtx, key)
				if err != nil {
					level.Error(util.Logger).Log("msg", "failed to get rule group", "key", key, "user", user)
					return err
				}

				mtx.Lock()
				result[user] = append(result[user], rg)
				mtx.Unlock()
			}

			return nil
		})
	}

	err := g.Wait()
	return result, err
}

func generateRuleObjectKey(id, namespace, name string) string {
	if id == "" {
		return rulePrefix
	}

	prefix := rulePrefix + id + "/"
	if namespace == "" {
		return prefix
	}

	ns := base64.URLEncoding.EncodeToString([]byte(namespace)) + "/"
	if name == "" {
		return prefix + ns
	}

	return prefix + ns + base64.URLEncoding.EncodeToString([]byte(name))
}

func decomposeRuleObjectKey(handle string) string {
	components := strings.Split(handle, "/")
	if len(components) != 4 {
		return ""
	}
	return components[1]
}
