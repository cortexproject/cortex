package local

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	promRules "github.com/prometheus/prometheus/rules"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/pkg/ruler/rulespb"
)

const (
	Name = "local"
)

type Config struct {
	Directory string `yaml:"directory"`
}

// RegisterFlags registers flags.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Directory, prefix+"local.directory", "", "Directory to scan for rules")
}

// Client expects to load already existing rules located at:
//  cfg.Directory / userID / namespace
type Client struct {
	cfg    Config
	loader promRules.GroupLoader
}

func NewLocalRulesClient(cfg Config, loader promRules.GroupLoader) (*Client, error) {
	if cfg.Directory == "" {
		return nil, errors.New("directory required for local rules config")
	}

	return &Client{
		cfg:    cfg,
		loader: loader,
	}, nil
}

func (l *Client) ListAllUsers(ctx context.Context) ([]string, error) {
	root := l.cfg.Directory
	infos, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read dir %s", root)
	}

	var result []string
	for _, info := range infos {
		// After resolving link, info.Name() may be different than user, so keep original name.
		user := info.Name()

		if info.Mode()&os.ModeSymlink != 0 {
			// ioutil.ReadDir only returns result of LStat. Calling Stat resolves symlink.
			info, err = os.Stat(filepath.Join(root, info.Name()))
			if err != nil {
				return nil, err
			}
		}

		if info.IsDir() {
			result = append(result, user)
		}
	}

	return result, nil
}

// ListAllRuleGroups implements rules.RuleStore. This method also loads the rules.
func (l *Client) ListAllRuleGroups(ctx context.Context) (map[string]rulespb.RuleGroupList, error) {
	users, err := l.ListAllUsers(ctx)
	if err != nil {
		return nil, err
	}

	lists := make(map[string]rulespb.RuleGroupList)
	for _, user := range users {
		list, err := l.loadAllRulesGroupsForUser(ctx, user)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to list rule groups for user %s", user)
		}

		lists[user] = list
	}

	return lists, nil
}

// ListRuleGroupsForUserAndNamespace implements rules.RuleStore. This method also loads the rules.
func (l *Client) ListRuleGroupsForUserAndNamespace(ctx context.Context, userID string, namespace string) (rulespb.RuleGroupList, error) {
	if namespace != "" {
		return l.loadAllRulesGroupsForUserAndNamespace(ctx, userID, namespace)
	}

	return l.loadAllRulesGroupsForUser(ctx, userID)
}

func (l *Client) LoadRuleGroups(_ context.Context, _ map[string]rulespb.RuleGroupList) error {
	// This Client already loads the rules in its List methods, there is nothing left to do here.
	return nil
}

// GetRuleGroup implements RuleStore
func (l *Client) GetRuleGroup(ctx context.Context, userID, namespace, group string) (*rulespb.RuleGroupDesc, error) {
	groups, err := l.loadAllRulesGroupsForUserAndNamespace(ctx, userID, namespace)
	if err != nil {
		return nil, err
	}

	for _, g := range groups {
		if g.Name == group {
			return g, nil
		}
	}

	return nil, errors.Errorf("group %s not found", group)
}

// SetRuleGroup implements RuleStore
func (l *Client) SetRuleGroup(ctx context.Context, userID, namespace string, group *rulespb.RuleGroupDesc) error {
	groups, err := l.loadAllRulesGroupsForUserAndNamespace(ctx, userID, namespace)
	if err != nil {
		return err
	}

	for i, g := range groups {
		if g.Name == group.Name {
			if g == group {
				// The rule group hasn't changed
				return nil
			}

			groups[i] = group
			return l.saveRuleGroupsForUserAndNamespace(userID, namespace, groups)
		}
	}

	groups = append(groups, group)

	return l.saveRuleGroupsForUserAndNamespace(userID, namespace, groups)
}

// DeleteRuleGroup implements RuleStore
func (l *Client) DeleteRuleGroup(ctx context.Context, userID, namespace, group string) error {
	groups, err := l.loadAllRulesGroupsForUserAndNamespace(ctx, userID, namespace)
	if err != nil {
		return err
	}

	for i, g := range groups {
		if g.Name == group {
			groups = append(groups[:i], groups[i+1:]...)
			return l.saveRuleGroupsForUserAndNamespace(userID, namespace, groups)
		}
	}

	return errors.Errorf("group %s not found", group)
}

// DeleteNamespace implements RulerStore
func (l *Client) DeleteNamespace(ctx context.Context, userID, namespace string) error {
	filename := filepath.Join(l.cfg.Directory, userID, namespace)
	return errors.Wrapf(os.Remove(filename), "error removing rule groups file %s", filename)
}

func (l *Client) loadAllRulesGroupsForUser(ctx context.Context, userID string) (rulespb.RuleGroupList, error) {
	var allLists rulespb.RuleGroupList

	root := filepath.Join(l.cfg.Directory, userID)

	_, err := os.Stat(root)
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.Wrapf(err, "unable to stat dir %s", root)
	} else if os.IsNotExist(err) {
		return rulespb.RuleGroupList{}, nil
	}

	infos, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read dir %s", root)
	}

	for _, info := range infos {
		// After resolving link, info.Name() may be different than namespace, so keep original name.
		namespace := info.Name()

		if info.Mode()&os.ModeSymlink != 0 {
			// ioutil.ReadDir only returns result of LStat. Calling Stat resolves symlink.
			info, err = os.Stat(filepath.Join(root, info.Name()))
			if err != nil {
				return nil, err
			}
		}

		if info.IsDir() {
			continue
		}

		list, err := l.loadAllRulesGroupsForUserAndNamespace(ctx, userID, namespace)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to list rule group for user %s and namespace %s", userID, namespace)
		}

		allLists = append(allLists, list...)
	}

	return allLists, nil
}

func (l *Client) loadAllRulesGroupsForUserAndNamespace(_ context.Context, userID string, namespace string) (rulespb.RuleGroupList, error) {
	filename := filepath.Join(l.cfg.Directory, userID, namespace)

	_, err := os.Stat(filename)
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.Wrapf(err, "unable to stat dir %s", filename)
	} else if os.IsNotExist(err) {
		return rulespb.RuleGroupList{}, nil
	}

	rulegroups, allErrors := l.loader.Load(filename)
	if len(allErrors) > 0 {
		return nil, errors.Wrapf(allErrors[0], "error parsing %s", filename)
	}

	var list rulespb.RuleGroupList

	for _, group := range rulegroups.Groups {
		desc := rulespb.ToProto(userID, namespace, group)
		list = append(list, desc)
	}

	return list, nil
}

func (l *Client) saveRuleGroupsForUserAndNamespace(userID, namespace string, ruleGroups rulespb.RuleGroupList) error {
	var list []rulefmt.RuleGroup
	for _, group := range ruleGroups {
		list = append(list, rulespb.FromProto(group))
	}

	g := rulefmt.RuleGroups{Groups: list}
	out, err := yaml.Marshal(g)
	if err != nil {
		return errors.Wrap(err, "unable to marshal rule groups")
	}

	filename := filepath.Join(l.cfg.Directory, userID, namespace)
	dir := filepath.Dir(filename)

	err = os.MkdirAll(dir, 07777)
	if err != nil {
		return errors.Wrapf(err, "error creating rule groups file dir %s", dir)
	}

	return errors.Wrapf(ioutil.WriteFile(filename, out, 0777), "error writing rule groups file %s", filename)
}
