package objectclient

import (
	"context"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/objtesting"

	"github.com/cortexproject/cortex/pkg/ruler/rules"
)

func TestBucketsImplementations(t *testing.T) {
	objtesting.ForeachStore(t, RuleStoreTest)
}

func RuleStoreTest(t *testing.T, bkt objstore.Bucket) {
	var (
		ctx         = context.Background()
		userOne     = "user1"
		userTwo     = "user2"
		namespace   = "namespace/one"
		interval, _ = time.ParseDuration("1m")
		mockRules   = map[string]rules.RuleGroupList{
			userOne: {
				&rules.RuleGroupDesc{
					Name:      "group/+1",
					Namespace: namespace,
					User:      userOne,
					Rules: []*rules.RuleDesc{
						{
							Record: "UP_RULE",
							Expr:   "up",
						},
						{
							Alert: "UP_ALERT",
							Expr:  "up < 1",
						},
					},
					Interval: interval,
				},
			},
			userTwo: {
				&rules.RuleGroupDesc{
					Name:      "group/+1",
					Namespace: namespace,
					User:      userTwo,
					Rules: []*rules.RuleDesc{
						{
							Record: "UP_RULE",
							Expr:   "up",
						},
					},
					Interval: interval,
				},
			},
		}
	)

	store := NewRuleStore(bkt)

	store.SetRuleGroup(ctx, userOne, namespace, mockRules[userOne][0])
}
