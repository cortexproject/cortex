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
			initService: mockInitFunc,
		},

		"serviceB": {
			deps:        []string{"serviceA"},
			initService: mockInitFunc,
		},

		"serviceC": {
			deps:        []string{"serviceB"},
			initService: mockInitFunc,
		},
	}

	mm := &moduleManager{}
	for name, mod := range testModules {
		mm.RegisterModule(name, mod.deps, mod.initService, nil)
	}
	svcs, err := mm.StartModule("serviceC")
	assert.NotNil(t, svcs)
	assert.NoError(t, err)

	invDeps := mm.findInverseDependencies("serviceB")
	assert.Len(t, invDeps, 1)
	assert.Equal(t, invDeps[0], "serviceC")
}
