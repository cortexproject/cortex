package ruler

import (
	"crypto/sha1"
	"encoding/hex"
)

type GroupStateDescs []*GroupStateDesc

func (gi GroupStateDescs) Swap(i, j int) { gi[i], gi[j] = gi[j], gi[i] }
func (gi GroupStateDescs) Less(i, j int) bool {
	return GetRuleGroupNextToken(gi[i].Group.Namespace, gi[i].Group.Name) < GetRuleGroupNextToken(gi[j].Group.Namespace, gi[j].Group.Name)
}
func (gi GroupStateDescs) Len() int { return len(gi) }

func GetRuleGroupNextToken(namespace string, group string) string {
	h := sha1.New()
	h.Write([]byte(namespace + ";" + group))
	return hex.EncodeToString(h.Sum(nil))
}

func TruncateGroups(groups []*GroupStateDesc, maxRuleGroups int) ([]*GroupStateDesc, string) {
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
