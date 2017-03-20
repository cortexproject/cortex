package chunk

import (
	"fmt"
	"sync"
)

// MockS3 is a fake S3Client.
type MockS3 struct {
	mtx     sync.RWMutex
	objects map[string][]byte
}

// NewMockS3 returns a new MockS3
func NewMockS3() *MockS3 {
	return &MockS3{
		objects: map[string][]byte{},
	}
}

// PutObject implements S3Client.
func (m *MockS3) PutObject(key string, buf []byte) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.objects[key] = buf
	return nil
}

// GetObject implements S3Client.
func (m *MockS3) GetObject(key string) ([]byte, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	buf, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("%v not found", key)
	}

	return buf, nil
}
