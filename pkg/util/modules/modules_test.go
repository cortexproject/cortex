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

func TestRegisterModuleDefaultsToPublic(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("module1", mockInitFunc)

	m := sut.modules["module1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.Equal(t, public, m.visibility, "module should be public")
}

func TestFunctionalOptAtTheEndWins(t *testing.T) {
	publicMod := func(option *module) {
		option.visibility = public
	}
	sut := NewManager()
	sut.RegisterModule("mod1", mockInitFunc, PrivateModule, publicMod, PrivateModule)

	m := sut.modules["mod1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.Equal(t, private, m.visibility, "module should be private")
}

func TestGetAllPublicModulesNames(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("public3", mockInitFunc)
	sut.RegisterModule("public2", mockInitFunc)
	sut.RegisterModule("public1", mockInitFunc)
	sut.RegisterModule("private1", mockInitFunc, PrivateModule)
	sut.RegisterModule("private2", mockInitFunc, PrivateModule)

	pm := sut.PublicModuleNames()

	assert.Equal(t, []string{"public1", "public2", "public3"}, pm, "module list contains wrong element and/or not sorted")
}

func TestGetAllPublicModulesNamesHasNoDupWithDependency(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("public1", mockInitFunc)
	sut.RegisterModule("public2", mockInitFunc)
	sut.RegisterModule("public3", mockInitFunc)

	assert.NoError(t, sut.AddDependency("public1", "public2", "public3"))

	pm := sut.PublicModuleNames()

	// make sure we don't include any module twice because there is a dependency
	assert.Equal(t, []string{"public1", "public2", "public3"}, pm, "module list contains wrong elements and/or not sorted")
}

func TestGetEmptyListWhenThereIsNoPublicModule(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("private1", mockInitFunc, PrivateModule)
	sut.RegisterModule("private2", mockInitFunc, PrivateModule)
	sut.RegisterModule("private3", mockInitFunc, PrivateModule)
	sut.RegisterModule("private4", mockInitFunc, PrivateModule)

	pm := sut.PublicModuleNames()

	assert.Len(t, pm, 0, "wrong result slice size")
}

func TestIsPublicModule(t *testing.T) {
	pubModName := "public"
	privateModName := "private"
	sut := NewManager()
	sut.RegisterModule(pubModName, mockInitFunc)
	sut.RegisterModule(privateModName, mockInitFunc, PrivateModule)

	var result = sut.IsPublicModule(pubModName)
	assert.True(t, result, "module '%v' should be public", pubModName)

	result = sut.IsPublicModule(privateModName)
	assert.False(t, result, "module '%v' should be private", privateModName)

	result = sut.IsPublicModule("ghost")
	assert.False(t, result, "expects result be false when module does not exist")
}
