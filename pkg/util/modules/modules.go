package modules

import (
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/util/services"
)

// module is the basic building block of the application
type module struct {
	// dependencies of this module
	deps []string

	// initFn for this module (can return nil)
	initFn func() (services.Service, error)

	// is this module public
	public bool
}

// Manager is a component that initialises modules of the application
// in the right order of dependencies.
type Manager struct {
	modules map[string]*module
}

// PrivateModule is an option for `RegisterModule` that marks module as private. Modules are public by default.
func PrivateModule(m *module) {
	m.public = false
}

// NewManager creates a new Manager
func NewManager() *Manager {
	return &Manager{
		modules: make(map[string]*module),
	}
}

// RegisterModule registers a new module with name, init function, and option functions.
// name must be unique to avoid overwriting modules
// if initFn is nil, the module will not initialise
// if options is not given, then by default module is public
func (m *Manager) RegisterModule(name string, initFn func() (services.Service, error), options ...func(option *module)) {
	m.modules[name] = &module{
		initFn: initFn,
		public: true,
	}

	for _, o := range options {
		o(m.modules[name])
	}
}

// AddDependency adds a dependency from name(source) to dependsOn(targets)
// An error is returned if the source module name is not found
func (m *Manager) AddDependency(name string, dependsOn ...string) error {
	if mod, ok := m.modules[name]; ok {
		mod.deps = append(mod.deps, dependsOn...)
	} else {
		return fmt.Errorf("no such module: %s", name)
	}
	return nil
}

// InitModuleServices initialises the target module by initialising all its dependencies
// in the right order. Modules are wrapped in such a way that they start after their
// dependencies have been started and stop before their dependencies are stopped.
func (m *Manager) InitModuleServices(target string) (map[string]services.Service, error) {
	if _, ok := m.modules[target]; !ok {
		return nil, fmt.Errorf("unrecognised module name: %s", target)
	}

	servicesMap := map[string]services.Service{}

	// initialize all of our dependencies first
	deps := m.orderedDeps(target)
	deps = append(deps, target) // lastly, initialize the requested module

	for ix, n := range deps {
		mod := m.modules[n]

		var serv services.Service

		if mod.initFn != nil {
			s, err := mod.initFn()
			if err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("error initialising module: %s", n))
			}

			if s != nil {
				// We pass servicesMap, which isn't yet complete. By the time service starts,
				// it will be fully built, so there is no need for extra synchronization.
				serv = newModuleServiceWrapper(servicesMap, n, s, mod.deps, m.findInverseDependencies(n, deps[ix+1:]))
			}
		}

		if serv != nil {
			servicesMap[n] = serv
		}
	}

	return servicesMap, nil
}

// PublicModuleNames gets list of module names that are
// public module.
func (m *Manager) PublicModuleNames() []string {
	var result []string
	for key, val := range m.modules {
		if val.public {
			result = append(result, key)
		}
	}

	sort.Strings(result)

	return result
}

// IsPublicModule check if given module is public or not. Returns true
// if and only if the given module is registered and is public.
func (m *Manager) IsPublicModule(mod string) bool {
	val, ok := m.modules[mod]

	if ok {
		return val.public
	}

	return false
}

// listDeps recursively gets a list of dependencies for a passed moduleName
func (m *Manager) listDeps(mod string) []string {
	deps := m.modules[mod].deps
	for _, d := range m.modules[mod].deps {
		deps = append(deps, m.listDeps(d)...)
	}
	return deps
}

// orderedDeps gets a list of all dependencies ordered so that items are always after any of their dependencies.
func (m *Manager) orderedDeps(mod string) []string {
	deps := m.listDeps(mod)

	// get a unique list of moduleNames, with a flag for whether they have been added to our result
	uniq := map[string]bool{}
	for _, dep := range deps {
		uniq[dep] = false
	}

	result := make([]string, 0, len(uniq))

	// keep looping through all modules until they have all been added to the result.

	for len(result) < len(uniq) {
	OUTER:
		for name, added := range uniq {
			if added {
				continue
			}
			for _, dep := range m.modules[name].deps {
				// stop processing this module if one of its dependencies has
				// not been added to the result yet.
				if !uniq[dep] {
					continue OUTER
				}
			}

			// if all of the module's dependencies have been added to the result slice,
			// then we can safely add this module to the result slice as well.
			uniq[name] = true
			result = append(result, name)
		}
	}
	return result
}

// find modules in the supplied list, that depend on mod
func (m *Manager) findInverseDependencies(mod string, mods []string) []string {
	result := []string(nil)

	for _, n := range mods {
		for _, d := range m.modules[n].deps {
			if d == mod {
				result = append(result, n)
				break
			}
		}
	}

	return result
}
