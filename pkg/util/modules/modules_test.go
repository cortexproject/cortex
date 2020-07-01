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

func TestRegisterModuleDefaultsToUserVisible(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("module1", mockInitFunc)

	m := sut.modules["module1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.True(t, m.userVisible, "module should be user visible")
}

func TestFunctionalOptAtTheEndWins(t *testing.T) {
	userVisibleMod := func(option *module) {
		option.userVisible = true
	}
	sut := NewManager()
	sut.RegisterModule("mod1", mockInitFunc, UserInvisibleModule, userVisibleMod, UserInvisibleModule)

	m := sut.modules["mod1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.False(t, m.userVisible, "module should be internal")
}

func TestGetAllUserVisibleModulesNames(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("userVisible3", mockInitFunc)
	sut.RegisterModule("userVisible2", mockInitFunc)
	sut.RegisterModule("userVisible1", mockInitFunc)
	sut.RegisterModule("internal1", mockInitFunc, UserInvisibleModule)
	sut.RegisterModule("internal2", mockInitFunc, UserInvisibleModule)

	pm := sut.UserVisibleModuleNames()

	assert.Equal(t, []string{"userVisible1", "userVisible2", "userVisible3"}, pm, "module list contains wrong element and/or not sorted")
}

func TestGetAllUserVisibleModulesNamesHasNoDupWithDependency(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("userVisible1", mockInitFunc)
	sut.RegisterModule("userVisible2", mockInitFunc)
	sut.RegisterModule("userVisible3", mockInitFunc)

	assert.NoError(t, sut.AddDependency("userVisible1", "userVisible2", "userVisible3"))

	pm := sut.UserVisibleModuleNames()

	// make sure we don't include any module twice because there is a dependency
	assert.Equal(t, []string{"userVisible1", "userVisible2", "userVisible3"}, pm, "module list contains wrong elements and/or not sorted")
}

func TestGetEmptyListWhenThereIsNoUserVisibleModule(t *testing.T) {
	sut := NewManager()
	sut.RegisterModule("internal1", mockInitFunc, UserInvisibleModule)
	sut.RegisterModule("internal2", mockInitFunc, UserInvisibleModule)
	sut.RegisterModule("internal3", mockInitFunc, UserInvisibleModule)
	sut.RegisterModule("internal4", mockInitFunc, UserInvisibleModule)

	pm := sut.UserVisibleModuleNames()

	assert.Len(t, pm, 0, "wrong result slice size")
}

func TestIsUserVisibleModule(t *testing.T) {
	userVisibleModName := "userVisible"
	internalModName := "internal"
	sut := NewManager()
	sut.RegisterModule(userVisibleModName, mockInitFunc)
	sut.RegisterModule(internalModName, mockInitFunc, UserInvisibleModule)

	var result = sut.IsUserVisibleModule(userVisibleModName)
	assert.True(t, result, "module '%v' should be user visible", userVisibleModName)

	result = sut.IsUserVisibleModule(internalModName)
	assert.False(t, result, "module '%v' should be internal", internalModName)

	result = sut.IsUserVisibleModule("ghost")
	assert.False(t, result, "expects result be false when module does not exist")
}
