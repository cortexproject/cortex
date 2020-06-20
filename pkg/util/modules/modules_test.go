package modules

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/services"
)

func mockInitFunc() (services.Service, error) { return nil, nil }

func TestDependencies(t *testing.T) {
	var testModules = map[string]module{
		"serviceA": {
			initFn: mockInitFunc,
		},

		"serviceB": {
			initFn: mockInitFunc,
		},

		"serviceC": {
			initFn: mockInitFunc,
		},
	}

	mm := NewManager()
	for name, mod := range testModules {
		mm.RegisterModule(name, mod.initFn)
	}
	assert.NoError(t, mm.AddDependency("serviceB", "serviceA"))
	assert.NoError(t, mm.AddDependency("serviceC", "serviceB"))
	assert.Equal(t, mm.modules["serviceB"].deps, []string{"serviceA"})

	invDeps := mm.findInverseDependencies("serviceA", []string{"serviceB", "serviceC"})
	require.Len(t, invDeps, 1)
	assert.Equal(t, invDeps[0], "serviceB")

	svcs, err := mm.InitModuleServices("serviceC")
	assert.NotNil(t, svcs)
	assert.NoError(t, err)

	svcs, err = mm.InitModuleServices("service_unknown")
	assert.Nil(t, svcs)
	assert.Error(t, err, fmt.Errorf("unrecognised module name: service_unknown"))
}

func TestCannotInitNonPublicModule(t *testing.T) {
	sut := NewManager()
	sut.RegisterModuleWithOption("module1", mockInitFunc, ModuleOption{Public: false})

	_, err := sut.InitModuleServices("module1")
	assert.Error(t, err, "Expect error when init private module")
}

func TestRegisterModuleWithOptions(t *testing.T) {
	option := ModuleOption{
		Public: true,
	}
	sut := NewManager()
	sut.RegisterModuleWithOption("module1", mockInitFunc, option)

	m := sut.modules["module1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.Equal(t, option, m.option, "option not assigned")
}

func TestRegisterModuleSetsDefaultOption(t *testing.T) {
	option := ModuleOption{
		Public: true,
	}
	sut := NewManager()
	sut.RegisterModule("module1", mockInitFunc)

	m := sut.modules["module1"]

	assert.NotNil(t, option, m.option, "option not assigned")
}

func TestGetAllPublicModulesNames(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("public1", mockInitFunc)
	sut.RegisterModuleWithOption("public2", mockInitFunc, ModuleOption{Public: true})
	sut.RegisterModuleWithOption("public3", mockInitFunc, ModuleOption{Public: true})
	sut.RegisterModuleWithOption("private1", mockInitFunc, ModuleOption{Public: false})
	sut.RegisterModuleWithOption("private2", mockInitFunc, ModuleOption{Public: false})

	pm := sut.PublicModuleNames()

	assert.Len(t, pm, 3, "wrong result slice size")
	assert.Contains(t, pm, "public1", "missing public module")
	assert.Contains(t, pm, "public2", "missing public module")
	assert.Contains(t, pm, "public3", "missing public module")
}

func TestGetAllPublicModulesNamesHasNoDupWithDependency(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("public1", mockInitFunc)
	sut.RegisterModule("public2", mockInitFunc)
	sut.RegisterModule("public3", mockInitFunc)

	assert.NoError(t, sut.AddDependency("public1", "public2", "public3"))

	pm := sut.PublicModuleNames()

	// make sure we don't include any module twice because there is a dependency
	assert.Len(t, pm, 3, "wrong result slice size")
	assert.Contains(t, pm, "public1", "missing public module")
	assert.Contains(t, pm, "public2", "missing public module")
	assert.Contains(t, pm, "public3", "missing public module")
}

func TestGetEmptyListWhenThereIsNoPublicModule(t *testing.T) {
	sut := NewManager()
	sut.RegisterModuleWithOption("private1", mockInitFunc, ModuleOption{Public: false})
	sut.RegisterModuleWithOption("private2", mockInitFunc, ModuleOption{Public: false})
	sut.RegisterModuleWithOption("private3", mockInitFunc, ModuleOption{Public: false})
	sut.RegisterModuleWithOption("private4", mockInitFunc, ModuleOption{Public: false})

	pm := sut.PublicModuleNames()

	assert.Len(t, pm, 0, "wrong result slice size")
}
