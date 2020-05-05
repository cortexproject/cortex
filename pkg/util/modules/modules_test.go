package modules

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/stretchr/testify/assert"
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

	mm := &Manager{}
	for name, mod := range testModules {
		mm.RegisterModule(name, mod.initFn)
	}
	mm.AddDependency("serviceB", "serviceA")
	mm.AddDependency("serviceC", "serviceB")
	svcs, err := mm.InitModuleServices("serviceC")
	assert.NotNil(t, svcs)
	assert.NoError(t, err)

	invDeps := mm.findInverseDependencies("serviceB", []string{"serviceA", "serviceC"})
	assert.Len(t, invDeps, 1)
	assert.Equal(t, invDeps[0], "serviceC")
}
