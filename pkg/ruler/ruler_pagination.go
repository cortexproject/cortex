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
// used to in subsequent requests. The # of groups per page is at most equal to maxRuleGroups. If the total passed in
// rule group count is greater than maxRuleGroups, then a next token is returned. Otherwise, next token is empty
func generatePage(groups []*GroupStateDesc, maxRuleGroups int) ([]*GroupStateDesc, string) {
	resultNumber := 0
	var returnPaginationToken string
	returnGroupDescs := make([]*GroupStateDesc, 0, len(groups))
	for _, groupInfo := range groups {

		// Add the rule group to the return slice if the maxRuleGroups is not hit
		if maxRuleGroups < 0 || resultNumber < maxRuleGroups {
			returnGroupDescs = append(returnGroupDescs, groupInfo)
			resultNumber++
			continue
		}

		// Return the next token if there are more groups
		if maxRuleGroups > 0 && resultNumber == maxRuleGroups {
			returnPaginationToken = GetRuleGroupNextToken(returnGroupDescs[maxRuleGroups-1].Group.Namespace, returnGroupDescs[maxRuleGroups-1].Group.Name)
			break
		}
	}
	return returnGroupDescs, returnPaginationToken
}
