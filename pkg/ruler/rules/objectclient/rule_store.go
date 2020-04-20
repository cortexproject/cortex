package objectclient

import (
	"bytes"
	"context"
	"io/ioutil"
	strings "strings"

	"github.com/go-kit/kit/log/level"
	proto "github.com/gogo/protobuf/proto"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/util"
)

// Object Rule Storage Schema
// =======================
// Object Name: "rules/<user_id>/<namespace>/<group_name>"
// Storage Format: Encoded RuleGroupDesc

const (
	rulePrefix = "rules/"
)

// RuleStore allows cortex rules to be stored using an object store backend.
type RuleStore struct {
	bucket objstore.Bucket
}

// NewRuleStore returns a new RuleStore
func NewRuleStore(bucket objstore.Bucket) *RuleStore {
	return &RuleStore{
		bucket: bucket,
	}
}

func (o *RuleStore) getRuleGroup(ctx context.Context, objectKey string) (*rules.RuleGroupDesc, error) {

	reader, err := o.bucket.Get(ctx, objectKey)
	if o.bucket.IsObjNotFoundErr(err) {
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
	userGroupMap := map[string]rules.RuleGroupList{}
	err := o.bucket.Iter(ctx, generateRuleObjectKey("", "", ""), func(s string) error {
		user := decomposeRuleObjectKey(s)
		if user == "" {
			return nil
		}

		rg, err := o.getRuleGroup(ctx, s)
		if err != nil {
			return err
		}

		if _, exists := userGroupMap[user]; !exists {
			userGroupMap[user] = rules.RuleGroupList{}
		}
		userGroupMap[user] = append(userGroupMap[user], rg)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return userGroupMap, nil
}

// ListRuleGroups returns all the active rule groups for a user
func (o *RuleStore) ListRuleGroups(ctx context.Context, userID, namespace string) (rules.RuleGroupList, error) {
	groups := []*rules.RuleGroupDesc{}

	err := o.bucket.Iter(ctx, generateRuleObjectKey(userID, namespace, ""), func(s string) error {
		level.Debug(util.Logger).Log("msg", "listing rule group", "key", s)

		rg, err := o.getRuleGroup(ctx, s)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to retrieve rule group", "err", err, "key", s)
			return err
		}
		groups = append(groups, rg)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return groups, nil
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
	return o.bucket.Upload(ctx, objectKey, bytes.NewReader(data))
}

// DeleteRuleGroup deletes the specified rule group
func (o *RuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, groupName string) error {
	objectKey := generateRuleObjectKey(userID, namespace, groupName)
	err := o.bucket.Delete(ctx, objectKey)
	if o.bucket.IsObjNotFoundErr(err) {
		return rules.ErrGroupNotFound
	}
	return err
}

func generateRuleObjectKey(id, namespace, name string) string {
	if id == "" {
		return rulePrefix
	}
	prefix := rulePrefix + id + "/"
	if namespace == "" {
		return prefix
	}
	return prefix + namespace + "/" + name
}

func decomposeRuleObjectKey(handle string) string {
	components := strings.Split(handle, "/")
	if len(components) != 4 {
		return ""
	}
	return components[1]
}
