package chunk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
)

type MockStorageMode int

var errPermissionDenied = errors.New("permission denied")

const (
	MockStorageModeReadWrite = 0
	MockStorageModeReadOnly  = 1
	MockStorageModeWriteOnly = 2
)

// MockStorage is a fake in-memory StorageClient.
type MockStorage struct {
	mtx     sync.RWMutex
	objects map[string][]byte

	mode MockStorageMode
}

// NewMockStorage creates a new MockStorage.
func NewMockStorage() *MockStorage {
	return &MockStorage{
		objects: map[string][]byte{},
	}
}

func (m *MockStorage) GetSortedObjectKeys() []string {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (m *MockStorage) GetObjectCount() int {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	return len(m.objects)
}

// Stop doesn't do anything.
func (*MockStorage) Stop() {
}

func (m *MockStorage) SetMode(mode MockStorageMode) {
	m.mode = mode
}

func (m *MockStorage) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	if m.mode == MockStorageModeWriteOnly {
		return nil, errPermissionDenied
	}

	buf, ok := m.objects[objectKey]
	if !ok {
		return nil, ErrStorageObjectNotFound
	}

	return ioutil.NopCloser(bytes.NewReader(buf)), nil
}

func (m *MockStorage) PutObject(ctx context.Context, objectKey string, object io.ReadSeeker) error {
	buf, err := ioutil.ReadAll(object)
	if err != nil {
		return err
	}

	if m.mode == MockStorageModeReadOnly {
		return errPermissionDenied
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.objects[objectKey] = buf
	return nil
}

func (m *MockStorage) DeleteObject(ctx context.Context, objectKey string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.mode == MockStorageModeReadOnly {
		return errPermissionDenied
	}

	if _, ok := m.objects[objectKey]; !ok {
		return ErrStorageObjectNotFound
	}

	delete(m.objects, objectKey)
	return nil
}

// List implements chunk.ObjectClient.
func (m *MockStorage) List(ctx context.Context, prefix, delimiter string) ([]StorageObject, []StorageCommonPrefix, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	if m.mode == MockStorageModeWriteOnly {
		return nil, nil, errPermissionDenied
	}

	prefixes := map[string]struct{}{}

	storageObjects := make([]StorageObject, 0, len(m.objects))
	for key := range m.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		// ToDo: Store mtime when we have mtime based use-cases for storage objects
		if delimiter == "" {
			storageObjects = append(storageObjects, StorageObject{Key: key})
			continue
		}

		ix := strings.Index(key[len(prefix):], delimiter)
		if ix < 0 {
			storageObjects = append(storageObjects, StorageObject{Key: key})
			continue
		}

		commonPrefix := key[:len(prefix)+ix+len(delimiter)] // Include delimeter in the common prefix.
		prefixes[commonPrefix] = struct{}{}
	}

	var commonPrefixes = []StorageCommonPrefix(nil)
	for p := range prefixes {
		commonPrefixes = append(commonPrefixes, StorageCommonPrefix(p))
	}

	// Object stores return results in sorted order.
	sort.Slice(storageObjects, func(i, j int) bool {
		return storageObjects[i].Key < storageObjects[j].Key
	})
	sort.Slice(commonPrefixes, func(i, j int) bool {
		return commonPrefixes[i] < commonPrefixes[j]
	})

	return storageObjects, commonPrefixes, nil
}
