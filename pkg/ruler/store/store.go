package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/rules"
)

var (
	ErrGroupNotFound          = errors.New("group does not exist")
	ErrGroupNamespaceNotFound = errors.New("group namespace does not exist")
	ErrUserNotFound           = errors.New("no rule groups found for user")
)

// RulePoller is used to poll for recently updated rules
type RulePoller interface {
	PollRules(ctx context.Context) (map[string][]RuleGroup, error)
	Stop()
}

// RuleStoreConditions are used to filter retrieived results from a rule store
type RuleStoreConditions struct {
	// UserID specifies to only retrieve rules with this ID
	UserID string

	// Namespaces filters results only rule groups with the specified namespace
	// are retrieved
	Namespace string
}

// RuleStore is used to store and retrieve rules
type RuleStore interface {
	ListRuleGroups(ctx context.Context, options RuleStoreConditions) (RuleGroupList, error)
	GetRuleGroup(ctx context.Context, userID, namespace, group string) (RuleGroup, error)
	SetRuleGroup(ctx context.Context, userID, namespace string, group rulefmt.RuleGroup) error
	DeleteRuleGroup(ctx context.Context, userID, namespace string, group string) error
}

// RuleGroup is used to retrieve rules from the database to evaluate,
// an interface is used to allow for lazy evaluation implementations
type RuleGroup interface {
	Rules(ctx context.Context) ([]rules.Rule, error)
	ID() string
	Name() string
	Namespace() string
	User() string
	Formatted() rulefmt.RuleGroup
}

type RuleGroupList []RuleGroup

func (l RuleGroupList) Formatted(user string) map[string][]rulefmt.RuleGroup {
	ruleMap := map[string][]rulefmt.RuleGroup{}
	for _, g := range l {
		if g.User() != user {
			continue
		}

		if _, exists := ruleMap[g.Namespace()]; !exists {
			ruleMap[g.Namespace()] = []rulefmt.RuleGroup{g.Formatted()}
		}
		ruleMap[g.Namespace()] = append(ruleMap[g.Namespace()], g.Formatted())

	}
	return ruleMap
}

// RuleNamespace is used to parse a slightly modified prometheus
// rule file format, if no namespace is set, the default namespace
// is used
type RuleNamespace struct {
	// Namespace field only exists for setting namespace in namespace body instead of file name
	Namespace string `yaml:"namespace,omitempty"`

	Groups []rulefmt.RuleGroup `yaml:"groups"`
}

// Validate each rule in the rule namespace is valid
func (r RuleNamespace) Validate() []error {
	set := map[string]struct{}{}
	var errs []error

	for _, g := range r.Groups {
		if g.Name == "" {
			errs = append(errs, fmt.Errorf("Groupname should not be empty"))
		}

		if _, ok := set[g.Name]; ok {
			errs = append(
				errs,
				fmt.Errorf("groupname: \"%s\" is repeated in the same namespace", g.Name),
			)
		}

		set[g.Name] = struct{}{}

		errs = append(errs, ValidateRuleGroup(g)...)
	}

	return errs
}

// ValidateRuleGroup validates a rulegroup
func ValidateRuleGroup(g rulefmt.RuleGroup) []error {
	var errs []error
	for i, r := range g.Rules {
		for _, err := range r.Validate() {
			var ruleName string
			if r.Alert != "" {
				ruleName = r.Alert
			} else {
				ruleName = r.Record
			}
			errs = append(errs, &rulefmt.Error{
				Group:    g.Name,
				Rule:     i,
				RuleName: ruleName,
				Err:      err,
			})
		}
	}

	return errs
}
