package ruler

import (
	"crypto/sha1"
	"encoding/hex"
)

type PaginatedGroupStates []*GroupStateDesc

func (gi PaginatedGroupStates) Swap(i, j int) { gi[i], gi[j] = gi[j], gi[i] }
func (gi PaginatedGroupStates) Less(i, j int) bool {
	return GetRuleGroupNextToken(gi[i].Group.Namespace, gi[i].Group.Name) < GetRuleGroupNextToken(gi[j].Group.Namespace, gi[j].Group.Name)
}
func (gi PaginatedGroupStates) Len() int { return len(gi) }

func GetRuleGroupNextToken(namespace string, group string) string {
	h := sha1.New()
	h.Write([]byte(namespace + ";" + group))
	return hex.EncodeToString(h.Sum(nil))
}

// generatePage function takes in a sorted list of groups and returns a page of groups and the next token which can be
// used to in subsequent requests. The # of groups per page is at most equal to maxRuleGroups and the number of rules is
// at most maxRules, unless one rulegroup contains more rules, then that entire rulegroup is returned.
// If the rule or rule group count is greater than their limit, a next token is returned. Otherwise, next token is empty
func generatePage(groups []*GroupStateDesc, maxRuleGroups int, maxRules uint) ([]*GroupStateDesc, string) {
	var returnPaginationToken string
	returnGroupDescs := make([]*GroupStateDesc, 0, len(groups))
	resultNumber := 0
	ruleCount := 0
	truncated := false

	for _, groupInfo := range groups {
		ruleLimit := maxRules > 0 && uint(ruleCount+len(groupInfo.ActiveRules)) > maxRules
		groupLimit := maxRuleGroups > 0 && resultNumber >= maxRuleGroups

		// Add the rule group to the return slice if the maxRules and maxRuleGroups is not hit, or if the first rulegroup exceeds maxRules
		if (!groupLimit && !ruleLimit) || (ruleLimit && resultNumber == 0) {
			returnGroupDescs = append(returnGroupDescs, groupInfo)
			ruleCount += len(groupInfo.ActiveRules)
			resultNumber++
		} else {
			truncated = true
			break
		}
	}

	// Return the next token if there are more groups. The guard above ensures resultNumber can never be 0 if truncated==true
	if truncated {
		returnPaginationToken = GetRuleGroupNextToken(returnGroupDescs[resultNumber-1].Group.Namespace, returnGroupDescs[resultNumber-1].Group.Name)
	}
	return returnGroupDescs, returnPaginationToken
}
