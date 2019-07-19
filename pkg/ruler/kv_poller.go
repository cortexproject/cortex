package ruler

import (
	"context"

	"github.com/cortexproject/cortex/pkg/ruler/store"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/usertracker"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/pkg/rulefmt"
)

type trackedPoller struct {
	tracker *usertracker.Tracker
	store   store.RuleStore

	initialized bool
}

func newTrackedPoller(tracker *usertracker.Tracker, store store.RuleStore) (*trackedPoller, error) {
	return &trackedPoller{
		tracker: tracker,
		store:   store,

		initialized: false,
	}, nil
}

func (p *trackedPoller) trackedRuleStore() *trackedRuleStore {
	return &trackedRuleStore{
		tracker: p.tracker,
		store:   p.store,
	}
}

func (p *trackedPoller) PollRules(ctx context.Context) (map[string][]store.RuleGroup, error) {
	updatedRules := map[string][]store.RuleGroup{}

	level.Debug(util.Logger).Log("msg", "polling for new rules")

	// First poll will return all rule groups
	if !p.initialized {
		level.Debug(util.Logger).Log("msg", "first poll, loading all rules")
		rgs, err := p.store.ListRuleGroups(ctx, store.RuleStoreConditions{})
		if err != nil {
			return nil, err
		}
		for _, rg := range rgs {
			if _, exists := updatedRules[rg.User()]; !exists {
				updatedRules[rg.User()] = []store.RuleGroup{rg}
			} else {
				updatedRules[rg.User()] = append(updatedRules[rg.User()], rg)
			}
		}
		p.initialized = true
	} else {
		users := p.tracker.GetUpdatedUsers(ctx)
		for _, u := range users {
			level.Debug(util.Logger).Log("msg", "poll found updated user", "user", u)
			rgs, err := p.store.ListRuleGroups(ctx, store.RuleStoreConditions{
				UserID: u,
			})
			if err != nil {
				return nil, err
			}

			updatedRules[u] = rgs
		}
	}

	return updatedRules, nil
}

func (p *trackedPoller) Stop() {
	p.tracker.Stop()
}

type trackedRuleStore struct {
	tracker *usertracker.Tracker
	store   store.RuleStore
}

// ListRuleGroups returns set of all rule groups matching the provided conditions
func (w *trackedRuleStore) ListRuleGroups(ctx context.Context, options store.RuleStoreConditions) (store.RuleGroupList, error) {
	return w.store.ListRuleGroups(ctx, options)
}

// GetRuleGroup retrieves the specified rule group from the backend store
func (w *trackedRuleStore) GetRuleGroup(ctx context.Context, userID, namespace, group string) (store.RuleGroup, error) {
	return w.store.GetRuleGroup(ctx, userID, namespace, group)
}

// SetRuleGroup updates a rule group in the backend persistent store, then it pushes a change update to the
// userID key entry in the KV store
func (w *trackedRuleStore) SetRuleGroup(ctx context.Context, userID, namespace string, group rulefmt.RuleGroup) error {
	err := w.store.SetRuleGroup(ctx, userID, namespace, group)
	if err != nil {
		return err
	}

	return w.tracker.UpdateUser(ctx, userID)
}

// DeleteRuleGroup deletes a rule group in the backend persistent store, then it pushes a change update to the
// userID key entry in the KV store
func (w *trackedRuleStore) DeleteRuleGroup(ctx context.Context, userID, namespace string, group string) error {
	err := w.store.DeleteRuleGroup(ctx, userID, namespace, group)
	if err != nil {
		return err
	}

	return w.tracker.UpdateUser(ctx, userID)
}
