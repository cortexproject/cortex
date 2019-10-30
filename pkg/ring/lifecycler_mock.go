package ring

import "github.com/stretchr/testify/mock"

// ReadLifecyclerMock mocks ReadLifecycler
type ReadLifecyclerMock struct {
	mock.Mock
}

// NewReadLifecyclerMock makes ReadLifecyclerMock
func NewReadLifecyclerMock() *ReadLifecyclerMock {
	return &ReadLifecyclerMock{}
}

// HealthyInstancesCount mocks lifecycler's HealthyInstancesCount
func (m *ReadLifecyclerMock) HealthyInstancesCount() int {
	args := m.Called()
	return args.Int(0)
}
