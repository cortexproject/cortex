package local

import (
	"context"
	"flag"
	"io/ioutil"
	"path"

	"github.com/pkg/errors"

	rulefmt "github.com/cortexproject/cortex/pkg/ruler/legacy_rulefmt"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
)

type Config struct {
	Directory string `yaml:"directory"`
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Directory, prefix+"local.directory", "", "Directory to scan for rules")
}

// LocalClient expects to load already existing rules located at:
//  cfg.Directory / userID / namespace
type LocalClient struct {
	cfg Config
}

func NewLocalRulesClient(cfg Config) (*LocalClient, error) {
	if cfg.Directory == "" {
		return nil, errors.New("directory required for local rules config")
	}

	return &LocalClient{
		cfg: cfg,
	}, nil
}

// ListAllRuleGroups implements RuleStore
func (l *LocalClient) ListAllRuleGroups(ctx context.Context) (map[string]rules.RuleGroupList, error) {
	lists := make(map[string]rules.RuleGroupList)

	root := l.cfg.Directory
	infos, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read dir "+root)
	}

	for _, info := range infos {
		if !info.IsDir() {
			continue
		}

		list, err := l.listAllRulesGroupsForUser(ctx, info.Name())
		if err != nil {
			return nil, errors.Wrap(err, "failed to list rule group")
		}

		lists[info.Name()] = list
	}

	return lists, nil
}

// ListRuleGroups implements RuleStore
func (l *LocalClient) ListRuleGroups(ctx context.Context, userID string, namespace string) (rules.RuleGroupList, error) {
	if namespace != "" {
		return l.listAllRulesGroupsForUserAndNamespace(ctx, userID, namespace)
	}

	return l.listAllRulesGroupsForUser(ctx, userID)
}

// GetRuleGroup implements RuleStore
func (l *LocalClient) GetRuleGroup(ctx context.Context, userID, namespace, group string) (*rules.RuleGroupDesc, error) {
	return nil, errors.New("GetRuleGroup unsupported in rule local store")
}

// SetRuleGroup implements RuleStore
func (l *LocalClient) SetRuleGroup(ctx context.Context, userID, namespace string, group *rules.RuleGroupDesc) error {
	return errors.New("SetRuleGroup unsupported in rule local store")
}

// DeleteRuleGroup implements RuleStore
func (l *LocalClient) DeleteRuleGroup(ctx context.Context, userID, namespace string, group string) error {
	return errors.New("DeleteRuleGroup unsupported in rule local store")
}

func (l *LocalClient) listAllRulesGroupsForUser(ctx context.Context, userID string) (rules.RuleGroupList, error) {
	var allLists rules.RuleGroupList

	root := path.Join(l.cfg.Directory, userID)
	infos, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read dir "+root)
	}

	for _, info := range infos {
		if info.IsDir() {
			continue
		}

		list, err := l.ListRuleGroups(ctx, userID, info.Name())
		if err != nil {
			return nil, errors.Wrap(err, "failed to list rule group")
		}

		allLists = append(allLists, list...)
	}

	return allLists, nil
}

func (l *LocalClient) listAllRulesGroupsForUserAndNamespace(ctx context.Context, userID string, namespace string) (rules.RuleGroupList, error) {
	filename := path.Join(l.cfg.Directory, userID, namespace)

	rulegroups, allErrors := rulefmt.ParseFile(filename)
	if len(allErrors) > 0 {
		return nil, errors.Wrap(allErrors[0], "error parsing "+filename)
	}

	allErrors = rulegroups.Validate()
	if len(allErrors) > 0 {
		return nil, errors.Wrap(allErrors[0], "error validating "+filename)
	}

	var list rules.RuleGroupList

	for _, group := range rulegroups.Groups {
		desc := rules.ToProto(userID, namespace, group)
		list = append(list, desc)
	}

	return list, nil
}
