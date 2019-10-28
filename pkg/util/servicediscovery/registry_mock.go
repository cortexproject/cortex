package servicediscovery

import "github.com/stretchr/testify/mock"

type ReadRegistryMock struct {
	mock.Mock
}

// NewReadRegistryMock makes a new ReadRegistry mock
func NewReadRegistryMock() *ReadRegistryMock {
	return &ReadRegistryMock{}
}

func (r *ReadRegistryMock) HealthyCount() int {
	args := r.Called()
	return args.Int(0)
}
