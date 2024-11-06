package ruler

import (
	"sort"
	"time"

	promRules "github.com/prometheus/prometheus/rules"
)

// mergeGroupStateDesc removes duplicates from the provided []*GroupStateDesc by keeping the GroupStateDesc with the
// latest information. It uses the EvaluationTimestamp of the GroupStateDesc and the EvaluationTimestamp of the
// ActiveRules in a GroupStateDesc to determine the which GroupStateDesc has the latest information.
// It also truncates rule groups if maxRuleGroups > 0
func mergeGroupStateDesc(ruleResponses []*RulesResponse, maxRuleGroups int32, dedup bool) *RulesResponse {

	var groupsStateDescs []*GroupStateDesc

	for _, resp := range ruleResponses {
		groupsStateDescs = append(groupsStateDescs, resp.Groups...)
	}

	states := make(map[string]*GroupStateDesc)
	rgTime := make(map[string]time.Time)
	groups := make([]*GroupStateDesc, 0)
	if dedup {
		for _, state := range groupsStateDescs {
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
		for _, state := range states {
			groups = append(groups, state)
		}
	} else {
		groups = groupsStateDescs
	}

	if maxRuleGroups > 0 {
		//Need to sort here before we truncate
		sort.Sort(PaginatedGroupStates(groups))
		result, nextToken := generatePage(groups, int(maxRuleGroups))
		return &RulesResponse{
			Groups:    result,
			NextToken: nextToken,
		}
	}
	return &RulesResponse{
		Groups:    groups,
		NextToken: "",
	}
}
