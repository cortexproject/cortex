package ruler

import (
	"crypto/md5"
	"sort"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

// mapper is designed to enusre the provided rule sets are identical
// to the on-disk rules tracked by the prometheus manager
type mapper struct {
	Path string

	FS afero.Fs
}

func newMapper(path string) *mapper {
	return &mapper{
		Path: path,
		FS:   afero.NewOsFs(),
	}
}

func (m *mapper) MapRules(user string, ruleConfigs map[string][]rulefmt.RuleGroup) (bool, []string, error) {
	var update bool

	// user rule files will be stored as `/<path>/<userid>/filename`
	path := m.Path + "/" + user + "/"
	err := m.FS.MkdirAll(path, 0777)
	if err != nil {
		return false, nil, err
	}

	filenames := []string{}

	// iterate through each rule group and map to disk if updated
	for f, groups := range ruleConfigs {
		// ensure rule groups are sorted before writing the file to disk
		sort.Slice(groups, func(i, j int) bool {
			return groups[i].Name > groups[j].Name
		})

		rgs := rulefmt.RuleGroups{Groups: groups}
		file := path + f
		d, err := yaml.Marshal(&rgs)
		if err != nil {
			return false, nil, err
		}

		// Determine if the file exists and whether it is identical to the current rule file
		if _, err := m.FS.Stat(file); err == nil {
			current, err := afero.ReadFile(m.FS, file)
			if err != nil {
				level.Warn(util.Logger).Log("msg", "unable to read rule file on disk", "file", file, "err", err)
				continue
			}
			newHash := md5.New()
			currentHash := md5.New()
			if string(currentHash.Sum(current)) == string(newHash.Sum(d)) {
				continue
			}
		}

		level.Info(util.Logger).Log("msg", "updating rule file", "file", file)
		err = afero.WriteFile(m.FS, file, d, 0777)
		if err != nil {
			return false, nil, err
		}
		update = true
		filenames = append(filenames, file)
	}
	return update, filenames, nil
}
