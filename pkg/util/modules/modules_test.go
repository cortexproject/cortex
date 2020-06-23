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

func TestRegisterModuleWithOptions(t *testing.T) {
	publicMod := func(option *ModuleOption) {
		option.Public = true
	}
	sut := NewManager()
	sut.RegisterModule("module1", mockInitFunc, publicMod)

	m := sut.modules["module1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.True(t, m.option.Public, "module should be public")
}

func TestRegisterModuleSetsDefaultOption(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("module1", mockInitFunc)

	m := sut.modules["module1"]

	assert.True(t, m.option.Public, "mould should be public")
}

func TestFunctionalOptAtTheEndWins(t *testing.T) {
	privateMod := func(option *ModuleOption) {
		option.Public = false
	}
	publicMod := func(option *ModuleOption) {
		option.Public = false
	}
	sut := NewManager()
	sut.RegisterModule("mod1", mockInitFunc, privateMod, publicMod, privateMod)

	m := sut.modules["mod1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.False(t, m.option.Public, "module should be public")
}

func TestGetAllPublicModulesNames(t *testing.T) {
	publicMod := func(option *ModuleOption) {
		option.Public = true
	}
	privateMod := func(option *ModuleOption) {
		option.Public = false
	}
	sut := NewManager()
	sut.RegisterModule("public1", mockInitFunc)
	sut.RegisterModule("public2", mockInitFunc, publicMod)
	sut.RegisterModule("public3", mockInitFunc, publicMod)
	sut.RegisterModule("private1", mockInitFunc, privateMod)
	sut.RegisterModule("private2", mockInitFunc, privateMod)

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
	privateMod := func(option *ModuleOption) {
		option.Public = false
	}
	sut := NewManager()
	sut.RegisterModule("private1", mockInitFunc, privateMod)
	sut.RegisterModule("private2", mockInitFunc, privateMod)
	sut.RegisterModule("private3", mockInitFunc, privateMod)
	sut.RegisterModule("private4", mockInitFunc, privateMod)

	pm := sut.PublicModuleNames()

	assert.Len(t, pm, 0, "wrong result slice size")
}

func TestPublicModuleNamesReturnsSortedList(t *testing.T) {
	publicMod := func(option *ModuleOption) {
		option.Public = true
	}
	sut := NewManager()
	sut.RegisterModule("c", mockInitFunc, publicMod)
	sut.RegisterModule("b", mockInitFunc, publicMod)
	sut.RegisterModule("a", mockInitFunc, publicMod)

	pm := sut.PublicModuleNames()

	assert.Len(t, pm, 3, "wrong result slice size")
	assert.Equal(t, []string{"a", "b", "c"}, pm, "module names list is not sorted in ascending order")
}

func TestIsPublicModule(t *testing.T) {
	publicMod := func(option *ModuleOption) {
		option.Public = true
	}
	privateMod := func(option *ModuleOption) {
		option.Public = false
	}
	pubModName := "public"
	privateModName := "private"
	sut := NewManager()
	sut.RegisterModule(pubModName, mockInitFunc, publicMod)
	sut.RegisterModule(privateModName, mockInitFunc, privateMod)

	var result = sut.IsPublicModule(pubModName)
	assert.True(t, result, "module '%v' should be public", pubModName)

	result = sut.IsPublicModule(privateModName)
	assert.False(t, result, "module '%v' should be private", privateModName)

	result = sut.IsPublicModule("ghost")
	assert.False(t, result, "expects result be false when module does not exist")
}
