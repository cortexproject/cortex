package servicediscovery

import "github.com/stretchr/testify/mock"

// ReadRegistryMock mocks ReadRegistry
type ReadRegistryMock struct {
	mock.Mock
}

// NewReadRegistryMock makes a new ReadRegistry mock
func NewReadRegistryMock() *ReadRegistryMock {
	return &ReadRegistryMock{}
}

// HealthyCount mocks ReadRegistry's HealthyCount
func (r *ReadRegistryMock) HealthyCount() int {
	args := r.Called()
	return args.Int(0)
}
