package rulestore

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/thanos-io/thanos/pkg/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

const (
	delim      = "/"
	rulePrefix = "rules"

	loadConcurrency = 10
)

// BucketRuleStore is used to support the RuleStore interface against an object storage backend. It is implemented
// using the Thanos objstore.Bucket interface
type BucketRuleStore struct {
	bucket      objstore.Bucket
	cfgProvider bucket.TenantConfigProvider
	logger      log.Logger
}

func NewBucketRuleStore(bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger) *BucketRuleStore {
	return &BucketRuleStore{
		bucket:      bucket.NewPrefixedBucketClient(bkt, rulePrefix),
		cfgProvider: cfgProvider,
		logger:      logger,
	}
}

func (b *BucketRuleStore) listNamespacesForUser(ctx context.Context, user string) ([]string, error) {
	userBucket := bucket.NewUserBucketClient(user, b.bucket, b.cfgProvider)

	var namespaces []string
	err := userBucket.Iter(ctx, "", func(namespace string) error {
		namespace = strings.TrimSuffix(namespace, delim)
		decodedNamespace, err := base64.URLEncoding.DecodeString(namespace)
		if err != nil {
			return fmt.Errorf("failed to decode namespace '%s': %w", namespace, err)
		}

		namespaces = append(namespaces, string(decodedNamespace))
		return nil
	})

	if err != nil {
		return nil, err
	}

	return namespaces, nil
}

// If existing rule group is supplied, it is Reset and reused. If nil, new RuleGroupDesc is allocated.
func (b *BucketRuleStore) getRuleGroup(ctx context.Context, userID, namespace, groupName string, rg *rules.RuleGroupDesc) (*rules.RuleGroupDesc, error) {
	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)
	objectKey := generateRuleObjectKey(namespace, groupName)

	reader, err := userBucket.Get(ctx, objectKey)
	if userBucket.IsObjNotFoundErr(err) {
		level.Debug(b.logger).Log("msg", "rule group does not exist", "name", objectKey)
		return nil, rules.ErrGroupNotFound
	}

	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	if rg == nil {
		rg = &rules.RuleGroupDesc{}
	} else {
		rg.Reset()
	}

	err = proto.Unmarshal(buf, rg)
	if err != nil {
		return nil, err
	}

	return rg, nil
}

func (b *BucketRuleStore) ListAllUsers(ctx context.Context) ([]string, error) {
	var users []string
	err := b.bucket.Iter(ctx, "", func(user string) error {
		users = append(users, strings.TrimSuffix(user, delim))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list users in rule store bucket: %w", err)
	}

	return users, nil
}

func (b *BucketRuleStore) ListAllRuleGroups(ctx context.Context) (map[string]rules.RuleGroupList, error) {
	users, err := b.ListAllUsers(ctx)
	if err != nil {
		return nil, err
	}

	perUserRuleGroupListMap := map[string]rules.RuleGroupList{}

	// TODO: Improve the following code path to run in parallel per-user
	for _, user := range users {
		perUserRuleGroupListMap[user], err = b.ListRuleGroupsForUserAndNamespace(ctx, user, "")
		if err != nil {
			return nil, err
		}
	}

	return perUserRuleGroupListMap, nil
}

// ListRuleGroupsForUserAndNamespace returns all the active rule groups for a user from given namespace.
// If namespace is empty, groups from all namespaces are returned.
func (b *BucketRuleStore) ListRuleGroupsForUserAndNamespace(ctx context.Context, userID string, namespace string) (rules.RuleGroupList, error) {
	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)

	var namespaces []string
	var err error

	if namespace != "" {
		namespaces = []string{namespace}
	} else {
		namespaces, err = b.listNamespacesForUser(ctx, userID)
		if err != nil {
			return nil, fmt.Errorf("failed to list namespaces for user %s: %w", userID, err)
		}
	}

	groupList := rules.RuleGroupList{}

	for _, namespace := range namespaces {
		prefix := generateRuleObjectKey(namespace, "")
		err = userBucket.Iter(ctx, prefix, func(group string) error {
			group = strings.TrimSuffix(strings.TrimPrefix(group, prefix), delim)
			decodedGroup, err := base64.URLEncoding.DecodeString(group)
			if err != nil {
				return fmt.Errorf("failed to decode group '%s': %w", group, err)
			}
			groupList = append(groupList, &rules.RuleGroupDesc{
				User:      userID,
				Namespace: namespace,
				Name:      string(decodedGroup),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return groupList, nil
}

// LoadRuleGroups loads rules for each rule group in the map.
// Parameter with groups to load *MUST* be coming from one of the List methods.
// Reason is that some implementations don't do anything, since their List method already loads the rules.
func (b *BucketRuleStore) LoadRuleGroups(ctx context.Context, groupsToLoad map[string]rules.RuleGroupList) error {
	ch := make(chan *rules.RuleGroupDesc)

	// Given we store one file per rule group. With this, we create a pool of workers that will
	// download all rule groups in parallel. We limit the number of workers to avoid a
	// particular user having too many rule groups rate limiting us with the object storage.
	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < loadConcurrency; i++ {
		g.Go(func() error {
			for gr := range ch {
				user, namespace, group := gr.GetUser(), gr.GetNamespace(), gr.GetName()
				if user == "" || namespace == "" || group == "" {
					return fmt.Errorf("invalid rule group: user=%q, namespace=%q, group=%q", user, namespace, group)
				}

				gr, err := b.getRuleGroup(gCtx, user, namespace, group, gr) // reuse group pointer from the map.
				if err != nil {
					return fmt.Errorf("failed to get rule group: user=%q, namespace=%q, name=%q, err=%w", user, namespace, group, err)
				}

				if user != gr.User || namespace != gr.Namespace || group != gr.Name {
					return fmt.Errorf("mismatch between requested rule group and loaded rule group, requested: user=%q, namespace=%q, group=%q, loaded: user=%q, namespace=%q, group=%q", user, namespace, group, gr.User, gr.Namespace, gr.Name)
				}
			}

			return nil
		})
	}

outer:
	for _, gs := range groupsToLoad {
		for _, g := range gs {
			if g == nil {
				continue
			}
			select {
			case <-gCtx.Done():
				break outer
			case ch <- g:
				// ok
			}
		}
	}
	close(ch)

	return g.Wait()
}

func (b *BucketRuleStore) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (*rules.RuleGroupDesc, error) {
	return b.getRuleGroup(ctx, userID, namespace, group, nil)
}

func (b *BucketRuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group *rules.RuleGroupDesc) error {
	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)
	data, err := proto.Marshal(group)
	if err != nil {
		return err
	}

	return userBucket.Upload(ctx, generateRuleObjectKey(namespace, group.Name), bytes.NewBuffer(data))
}

func (b *BucketRuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)
	err := userBucket.Delete(ctx, generateRuleObjectKey(namespace, group))
	if b.bucket.IsObjNotFoundErr(err) {
		return rules.ErrGroupNotFound
	}
	return err
}

func (b *BucketRuleStore) DeleteNamespace(ctx context.Context, userID string, namespace string) error {
	ruleGroupList, err := b.ListRuleGroupsForUserAndNamespace(ctx, userID, namespace)
	if err != nil {
		return err
	}

	if len(ruleGroupList) == 0 {
		return rules.ErrGroupNamespaceNotFound
	}

	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)
	for _, rg := range ruleGroupList {
		if err := ctx.Err(); err != nil {
			return err
		}
		objectKey := generateRuleObjectKey(rg.Namespace, rg.Name)
		level.Debug(b.logger).Log("msg", "deleting rule group", "namespace", namespace, "key", objectKey)
		err = userBucket.Delete(ctx, objectKey)
		if err != nil {
			level.Error(b.logger).Log("msg", "unable to delete rule group from namespace", "err", err, "namespace", namespace, "key", objectKey)
			return err
		}
	}

	return nil
}

func (b *BucketRuleStore) SupportsModifications() bool {
	return true
}

func generateRuleObjectKey(namespace, groupName string) string {
	ns := base64.URLEncoding.EncodeToString([]byte(namespace)) + delim
	if groupName == "" {
		return ns
	}

	return ns + base64.URLEncoding.EncodeToString([]byte(groupName))
}
