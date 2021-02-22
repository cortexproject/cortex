package rulestore

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"

	github_com_cortexproject_cortex_pkg_ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
)

// TODO: make the following test functions generic and share them with the existing objectclient

type testGroup struct {
	user, namespace string
	ruleGroup       rulefmt.RuleGroup
}

func TestListRules(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	rs := NewBucketRuleStore(bucket, nil, log.NewNopLogger())

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
		user2Groups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user2", "")
		require.NoError(t, err)
		require.ElementsMatch(t, []*rules.RuleGroupDesc{
			{User: "user2", Namespace: "+-!@#$%. ", Name: "different user"},
		}, user2Groups)
	}
}

func TestLoadRules(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	rs := NewBucketRuleStore(bucket, nil, log.NewNopLogger())

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
					Labels: []github_com_cortexproject_cortex_pkg_ingester_client.LabelAdapter{{Name: "label1", Value: "value1"}},
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
	require.EqualError(t, rs.LoadRuleGroups(context.Background(), allGroupsMap), "failed to get rule group: user=\"user2\", namespace=\"world\", name=\"first testGroup\", err=group does not exist")
}

func TestDelete(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	rs := NewBucketRuleStore(bucket, nil, log.NewNopLogger())

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

		expectedKeySet := map[string]struct{}{
			"rules/user1/" + generateRuleObjectKey("A", "1"):          {},
			"rules/user1/" + generateRuleObjectKey("A", "2"):          {},
			"rules/user1/" + generateRuleObjectKey("B", "3"):          {},
			"rules/user1/" + generateRuleObjectKey("C", "4"):          {},
			"rules/user2/" + generateRuleObjectKey("second", "group"): {},
			"rules/user3/" + generateRuleObjectKey("third", "group"):  {},
		}

		objs := bucket.Objects()
		for key := range objs {
			_, found := expectedKeySet[key]
			require.True(t, found)
		}
	}

	// Verify that we can delete individual rule group, or entire namespace.
	{
		require.NoError(t, rs.DeleteRuleGroup(context.Background(), "user2", "second", "group"))
		require.NoError(t, rs.DeleteNamespace(context.Background(), "user1", "A"))

		expectedKeySet := map[string]struct{}{
			"rules/user1/" + generateRuleObjectKey("B", "3"):         {},
			"rules/user1/" + generateRuleObjectKey("C", "4"):         {},
			"rules/user3/" + generateRuleObjectKey("third", "group"): {},
		}

		objs := bucket.Objects()
		for key := range objs {
			_, found := expectedKeySet[key]
			require.True(t, found)
		}
	}

	// Verify that we can delete all remaining namespaces for user1.
	{
		require.NoError(t, rs.DeleteNamespace(context.Background(), "user1", ""))

		expectedKeySet := map[string]struct{}{
			"rules/user3/" + generateRuleObjectKey("third", "group"): {},
		}

		for key := range bucket.Objects() {
			_, found := expectedKeySet[key]
			require.True(t, found)
		}
	}

	{
		// Trying to delete empty namespace again will result in error.
		require.Equal(t, rules.ErrGroupNamespaceNotFound, rs.DeleteNamespace(context.Background(), "user1", ""))
	}
}
