package modules

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mockInitFunc() (services.Service, error) { return services.NewIdleService(nil, nil), nil }

func mockInitFuncFail() (services.Service, error) { return nil, errors.New("Error") }

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

		"serviceD": {
			initFn: mockInitFuncFail,
		},
	}

	mm := NewManager(log.NewNopLogger())
	for name, mod := range testModules {
		mm.RegisterModule(name, mod.initFn)
	}
	assert.NoError(t, mm.AddDependency("serviceB", "serviceA"))
	assert.NoError(t, mm.AddDependency("serviceC", "serviceB"))
	assert.Equal(t, mm.modules["serviceB"].deps, []string{"serviceA"})

	invDeps := mm.findInverseDependencies("serviceA", []string{"serviceB", "serviceC"})
	require.Len(t, invDeps, 1)
	assert.Equal(t, invDeps[0], "serviceB")

	// Test unknown module
	svc, err := mm.InitModuleServices("service_unknown")
	assert.Error(t, err, fmt.Errorf("unrecognised module name: service_unknown"))
	assert.Empty(t, svc)

	// Test init failure
	svc, err = mm.InitModuleServices("serviceD")
	assert.Error(t, err)
	assert.Empty(t, svc)

	// Test loading several modules
	svc, err = mm.InitModuleServices("serviceA", "serviceB")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(svc))

	svc, err = mm.InitModuleServices("serviceC")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(svc))

	// Test loading of the module second time - should produce the same set of services, but new instances.
	svc2, err := mm.InitModuleServices("serviceC")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(svc))
	assert.NotEqual(t, svc, svc2)
}

func TestRegisterModuleDefaultsToUserVisible(t *testing.T) {
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule("module1", mockInitFunc)

	m := sut.modules["module1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.True(t, m.userVisible, "module should be user visible")
}

func TestFunctionalOptAtTheEndWins(t *testing.T) {
	userVisibleMod := func(option *module) {
		option.userVisible = true
	}
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule("mod1", mockInitFunc, UserInvisibleModule, userVisibleMod, UserInvisibleModule)

	m := sut.modules["mod1"]

	assert.NotNil(t, mockInitFunc, m.initFn, "initFn not assigned")
	assert.False(t, m.userVisible, "module should be internal")
}

func TestGetAllUserVisibleModulesNames(t *testing.T) {
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule("userVisible3", mockInitFunc)
	sut.RegisterModule("userVisible2", mockInitFunc)
	sut.RegisterModule("userVisible1", mockInitFunc)
	sut.RegisterModule("internal1", mockInitFunc, UserInvisibleModule)
	sut.RegisterModule("internal2", mockInitFunc, UserInvisibleModule)

	pm := sut.UserVisibleModuleNames()

	assert.Equal(t, []string{"userVisible1", "userVisible2", "userVisible3"}, pm, "module list contains wrong element and/or not sorted")
}

func TestGetAllUserVisibleModulesNamesHasNoDupWithDependency(t *testing.T) {
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule("userVisible1", mockInitFunc)
	sut.RegisterModule("userVisible2", mockInitFunc)
	sut.RegisterModule("userVisible3", mockInitFunc)

	assert.NoError(t, sut.AddDependency("userVisible1", "userVisible2", "userVisible3"))

	pm := sut.UserVisibleModuleNames()

	// make sure we don't include any module twice because there is a dependency
	assert.Equal(t, []string{"userVisible1", "userVisible2", "userVisible3"}, pm, "module list contains wrong elements and/or not sorted")
}

func TestGetEmptyListWhenThereIsNoUserVisibleModule(t *testing.T) {
	sut := NewManager(log.NewNopLogger())
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
	sut := NewManager(log.NewNopLogger())
	sut.RegisterModule(userVisibleModName, mockInitFunc)
	sut.RegisterModule(internalModName, mockInitFunc, UserInvisibleModule)

	var result = sut.IsUserVisibleModule(userVisibleModName)
	assert.True(t, result, "module '%v' should be user visible", userVisibleModName)

	result = sut.IsUserVisibleModule(internalModName)
	assert.False(t, result, "module '%v' should be internal", internalModName)

	result = sut.IsUserVisibleModule("ghost")
	assert.False(t, result, "expects result be false when module does not exist")
}

func TestIsModuleRegistered(t *testing.T) {
	successModule := "successModule"
	failureModule := "failureModule"

	m := NewManager(log.NewNopLogger())
	m.RegisterModule(successModule, mockInitFunc)

	var result = m.IsModuleRegistered(successModule)
	assert.True(t, result, "module '%v' should be registered", successModule)

	result = m.IsModuleRegistered(failureModule)
	assert.False(t, result, "module '%v' should NOT be registered", failureModule)
}

func TestDependenciesForModule(t *testing.T) {
	m := NewManager(log.NewNopLogger())
	m.RegisterModule("test", nil)
	m.RegisterModule("dep1", nil)
	m.RegisterModule("dep2", nil)
	m.RegisterModule("dep3", nil)

	require.NoError(t, m.AddDependency("test", "dep2", "dep1"))
	require.NoError(t, m.AddDependency("dep1", "dep2"))
	require.NoError(t, m.AddDependency("dep2", "dep3"))

	deps := m.DependenciesForModule("test")
	assert.Equal(t, []string{"dep1", "dep2", "dep3"}, deps)
}

func TestModuleWaitsForAllDependencies(t *testing.T) {
	var serviceA services.Service

	initA := func() (services.Service, error) {
		serviceA = services.NewIdleService(func(serviceContext context.Context) error {
			// Slow-starting service. Delay is here to verify that service for C is not started before this service
			// has finished starting.
			time.Sleep(1 * time.Second)
			return nil
		}, nil)

		return serviceA, nil
	}

	initC := func() (services.Service, error) {
		return services.NewIdleService(func(serviceContext context.Context) error {
			// At this point, serviceA should be Running, because "C" depends (indirectly) on "A".
			if s := serviceA.State(); s != services.Running {
				return fmt.Errorf("serviceA has invalid state: %v", s)
			}
			return nil
		}, nil), nil
	}

	m := NewManager(log.NewNopLogger())
	m.RegisterModule("A", initA)
	m.RegisterModule("B", nil)
	m.RegisterModule("C", initC)

	// C -> B -> A. Even though B has no service, C must still wait for service A to start, before C can start.
	require.NoError(t, m.AddDependency("B", "A"))
	require.NoError(t, m.AddDependency("C", "B"))

	servsMap, err := m.InitModuleServices("C")
	require.NoError(t, err)

	// Build service manager from services, and start it.
	servs := []services.Service(nil)
	for _, s := range servsMap {
		servs = append(servs, s)
	}

	servManager, err := services.NewManager(servs...)
	require.NoError(t, err)
	assert.NoError(t, services.StartManagerAndAwaitHealthy(context.Background(), servManager))
	assert.NoError(t, services.StopManagerAndAwaitStopped(context.Background(), servManager))
}
