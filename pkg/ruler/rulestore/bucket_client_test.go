package rulestore

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/cortexproject/cortex/pkg/ruler/rules/objectclient"
)

type testGroup struct {
	user, namespace string
	ruleGroup       rulefmt.RuleGroup
}

func TestListRules(t *testing.T) {
	runForEachRuleStore(t, func(t *testing.T, rs rules.RuleStore, _ interface{}) {
		groups := []testGroup{
			{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "first testGroup"}},
			{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "second testGroup"}},
			{user: "user1", namespace: "world", ruleGroup: rulefmt.RuleGroup{Name: "another namespace testGroup"}},
			{user: "user2", namespace: "+-!@#$%. ", ruleGroup: rulefmt.RuleGroup{Name: "different user"}},
		}

		for _, g := range groups {
			desc := rules.ToProto(g.user, g.namespace, g.ruleGroup)
			require.NoError(t, rs.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
		}

		{
			users, err := rs.ListAllUsers(context.Background())
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"user1", "user2"}, users)
		}

		{
			allGroupsMap, err := rs.ListAllRuleGroups(context.Background())
			require.NoError(t, err)
			require.Len(t, allGroupsMap, 2)
			require.ElementsMatch(t, []*rules.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup"},
				{User: "user1", Namespace: "hello", Name: "second testGroup"},
				{User: "user1", Namespace: "world", Name: "another namespace testGroup"},
			}, allGroupsMap["user1"])
			require.ElementsMatch(t, []*rules.RuleGroupDesc{
				{User: "user2", Namespace: "+-!@#$%. ", Name: "different user"},
			}, allGroupsMap["user2"])
		}

		{
			user1Groups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "")
			require.NoError(t, err)
			require.ElementsMatch(t, []*rules.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup"},
				{User: "user1", Namespace: "hello", Name: "second testGroup"},
				{User: "user1", Namespace: "world", Name: "another namespace testGroup"},
			}, user1Groups)
		}

		{
			helloGroups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "hello")
			require.NoError(t, err)
			require.ElementsMatch(t, []*rules.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup"},
				{User: "user1", Namespace: "hello", Name: "second testGroup"},
			}, helloGroups)
		}

		{
			invalidUserGroups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "invalid", "")
			require.NoError(t, err)
			require.Empty(t, invalidUserGroups)
		}

		{
			invalidNamespaceGroups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "invalid")
			require.NoError(t, err)
			require.Empty(t, invalidNamespaceGroups)
		}

		{
			user2Groups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user2", "")
			require.NoError(t, err)
			require.ElementsMatch(t, []*rules.RuleGroupDesc{
				{User: "user2", Namespace: "+-!@#$%. ", Name: "different user"},
			}, user2Groups)
		}
	})
}

func TestLoadRules(t *testing.T) {
	runForEachRuleStore(t, func(t *testing.T, rs rules.RuleStore, _ interface{}) {
		groups := []testGroup{
			{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "first testGroup", Interval: model.Duration(time.Minute), Rules: []rulefmt.RuleNode{{
				For:    model.Duration(5 * time.Minute),
				Labels: map[string]string{"label1": "value1"},
			}}}},
			{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "second testGroup", Interval: model.Duration(2 * time.Minute)}},
			{user: "user1", namespace: "world", ruleGroup: rulefmt.RuleGroup{Name: "another namespace testGroup", Interval: model.Duration(1 * time.Hour)}},
			{user: "user2", namespace: "+-!@#$%. ", ruleGroup: rulefmt.RuleGroup{Name: "different user", Interval: model.Duration(5 * time.Minute)}},
		}

		for _, g := range groups {
			desc := rules.ToProto(g.user, g.namespace, g.ruleGroup)
			require.NoError(t, rs.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
		}

		allGroupsMap, err := rs.ListAllRuleGroups(context.Background())

		// Before load, rules are not loaded
		{
			require.NoError(t, err)
			require.Len(t, allGroupsMap, 2)
			require.ElementsMatch(t, []*rules.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup"},
				{User: "user1", Namespace: "hello", Name: "second testGroup"},
				{User: "user1", Namespace: "world", Name: "another namespace testGroup"},
			}, allGroupsMap["user1"])
			require.ElementsMatch(t, []*rules.RuleGroupDesc{
				{User: "user2", Namespace: "+-!@#$%. ", Name: "different user"},
			}, allGroupsMap["user2"])
		}

		err = rs.LoadRuleGroups(context.Background(), allGroupsMap)
		require.NoError(t, err)

		// After load, rules are loaded.
		{
			require.NoError(t, err)
			require.Len(t, allGroupsMap, 2)

			require.ElementsMatch(t, []*rules.RuleGroupDesc{
				{User: "user1", Namespace: "hello", Name: "first testGroup", Interval: time.Minute, Rules: []*rules.RuleDesc{
					{
						For:    5 * time.Minute,
						Labels: []cortexpb.LabelAdapter{{Name: "label1", Value: "value1"}},
					},
				}},
				{User: "user1", Namespace: "hello", Name: "second testGroup", Interval: 2 * time.Minute},
				{User: "user1", Namespace: "world", Name: "another namespace testGroup", Interval: 1 * time.Hour},
			}, allGroupsMap["user1"])

			require.ElementsMatch(t, []*rules.RuleGroupDesc{
				{User: "user2", Namespace: "+-!@#$%. ", Name: "different user", Interval: 5 * time.Minute},
			}, allGroupsMap["user2"])
		}

		// Loading group with mismatched info fails.
		require.NoError(t, rs.SetRuleGroup(context.Background(), "user1", "hello", &rules.RuleGroupDesc{User: "user2", Namespace: "world", Name: "first testGroup"}))
		require.EqualError(t, rs.LoadRuleGroups(context.Background(), allGroupsMap), "mismatch between requested rule group and loaded rule group, requested: user=\"user1\", namespace=\"hello\", group=\"first testGroup\", loaded: user=\"user2\", namespace=\"world\", group=\"first testGroup\"")

		// Load with missing rule groups fails.
		require.NoError(t, rs.DeleteRuleGroup(context.Background(), "user1", "hello", "first testGroup"))
		require.EqualError(t, rs.LoadRuleGroups(context.Background(), allGroupsMap), "get rule group user=\"user2\", namespace=\"world\", name=\"first testGroup\": group does not exist")
	})
}

func TestDelete(t *testing.T) {
	runForEachRuleStore(t, func(t *testing.T, rs rules.RuleStore, bucketClient interface{}) {
		groups := []testGroup{
			{user: "user1", namespace: "A", ruleGroup: rulefmt.RuleGroup{Name: "1"}},
			{user: "user1", namespace: "A", ruleGroup: rulefmt.RuleGroup{Name: "2"}},
			{user: "user1", namespace: "B", ruleGroup: rulefmt.RuleGroup{Name: "3"}},
			{user: "user1", namespace: "C", ruleGroup: rulefmt.RuleGroup{Name: "4"}},
			{user: "user2", namespace: "second", ruleGroup: rulefmt.RuleGroup{Name: "group"}},
			{user: "user3", namespace: "third", ruleGroup: rulefmt.RuleGroup{Name: "group"}},
		}

		for _, g := range groups {
			desc := rules.ToProto(g.user, g.namespace, g.ruleGroup)
			require.NoError(t, rs.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
		}

		// Verify that nothing was deleted, because we used canceled context.
		{
			canceled, cancelFn := context.WithCancel(context.Background())
			cancelFn()

			require.Error(t, rs.DeleteNamespace(canceled, "user1", ""))

			require.Equal(t, []string{
				"rules/user1/" + generateRuleObjectKey("A", "1"),
				"rules/user1/" + generateRuleObjectKey("A", "2"),
				"rules/user1/" + generateRuleObjectKey("B", "3"),
				"rules/user1/" + generateRuleObjectKey("C", "4"),
				"rules/user2/" + generateRuleObjectKey("second", "group"),
				"rules/user3/" + generateRuleObjectKey("third", "group"),
			}, getSortedObjectKeys(bucketClient))
		}

		// Verify that we can delete individual rule group, or entire namespace.
		{
			require.NoError(t, rs.DeleteRuleGroup(context.Background(), "user2", "second", "group"))
			require.NoError(t, rs.DeleteNamespace(context.Background(), "user1", "A"))

			require.Equal(t, []string{
				"rules/user1/" + generateRuleObjectKey("B", "3"),
				"rules/user1/" + generateRuleObjectKey("C", "4"),
				"rules/user3/" + generateRuleObjectKey("third", "group"),
			}, getSortedObjectKeys(bucketClient))
		}

		// Verify that we can delete all remaining namespaces for user1.
		{
			require.NoError(t, rs.DeleteNamespace(context.Background(), "user1", ""))

			require.Equal(t, []string{
				"rules/user3/" + generateRuleObjectKey("third", "group"),
			}, getSortedObjectKeys(bucketClient))
		}

		{
			// Trying to delete empty namespace again will result in error.
			require.Equal(t, rules.ErrGroupNamespaceNotFound, rs.DeleteNamespace(context.Background(), "user1", ""))
		}
	})
}

func runForEachRuleStore(t *testing.T, testFn func(t *testing.T, store rules.RuleStore, bucketClient interface{})) {
	legacyClient := chunk.NewMockStorage()
	legacyStore := objectclient.NewRuleStore(legacyClient, 5, log.NewNopLogger())

	bucketClient := objstore.NewInMemBucket()
	bucketStore := NewBucketRuleStore(bucketClient, nil, log.NewNopLogger())

	stores := map[string]struct {
		store  rules.RuleStore
		client interface{}
	}{
		"legacy": {store: legacyStore, client: legacyClient},
		"bucket": {store: bucketStore, client: bucketClient},
	}

	for name, data := range stores {
		t.Run(name, func(t *testing.T) {
			testFn(t, data.store, data.client)
		})
	}
}

func getSortedObjectKeys(bucketClient interface{}) []string {
	if typed, ok := bucketClient.(*chunk.MockStorage); ok {
		return typed.GetSortedObjectKeys()
	}

	if typed, ok := bucketClient.(*objstore.InMemBucket); ok {
		var keys []string
		for key := range typed.Objects() {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return keys
	}

	return nil
}

func TestParseRuleGroupObjectKey(t *testing.T) {
	decodedNamespace := "my-namespace"
	encodedNamespace := base64.URLEncoding.EncodeToString([]byte(decodedNamespace))

	decodedGroup := "my-group"
	encodedGroup := base64.URLEncoding.EncodeToString([]byte(decodedGroup))

	tests := map[string]struct {
		key               string
		expectedErr       error
		expectedNamespace string
		expectedGroup     string
	}{
		"empty object key": {
			key:         "",
			expectedErr: errInvalidRuleGroupKey,
		},
		"invalid object key pattern": {
			key:         "way/too/long",
			expectedErr: errInvalidRuleGroupKey,
		},
		"invalid namespace encoding": {
			key:         fmt.Sprintf("invalid/%s", encodedGroup),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"invalid group encoding": {
			key:         fmt.Sprintf("%s/invalid", encodedNamespace),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"valid object key": {
			key:               fmt.Sprintf("%s/%s", encodedNamespace, encodedGroup),
			expectedNamespace: decodedNamespace,
			expectedGroup:     decodedGroup,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			namespace, group, err := parseRuleGroupObjectKey(testData.key)

			if testData.expectedErr != nil {
				assert.EqualError(t, err, testData.expectedErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, testData.expectedNamespace, namespace)
				assert.Equal(t, testData.expectedGroup, group)
			}
		})
	}
}
