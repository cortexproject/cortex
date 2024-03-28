package ruler

import (
	"time"

	promRules "github.com/prometheus/prometheus/rules"
)

// mergeGroupStateDesc removes duplicates from the provided []*GroupStateDesc by keeping the GroupStateDesc with the
// latest information. It uses the EvaluationTimestamp of the GroupStateDesc and the EvaluationTimestamp of the
// ActiveRules in a GroupStateDesc to determine the which GroupStateDesc has the latest information.
func mergeGroupStateDesc(in []*GroupStateDesc) []*GroupStateDesc {
	states := make(map[string]*GroupStateDesc)
	rgTime := make(map[string]time.Time)
	for _, state := range in {
		latestTs := state.EvaluationTimestamp
		for _, r := range state.ActiveRules {
			if latestTs.Before(r.EvaluationTimestamp) {
				latestTs = r.EvaluationTimestamp
			}
		}
		key := promRules.GroupKey(state.Group.Namespace, state.Group.Name)
		ts, ok := rgTime[key]
		if !ok || ts.Before(latestTs) {
			states[key] = state
			rgTime[key] = latestTs
		}
	}
	groups := make([]*GroupStateDesc, 0, len(states))
	for _, state := range states {
		groups = append(groups, state)
	}
	return groups
}
