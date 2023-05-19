package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util/backoff"
)

const key = "test"

var (
	defaultBackoff = backoff.Config{
		MinBackoff: 1 * time.Millisecond,
		MaxBackoff: 1 * time.Millisecond,
		MaxRetries: 0,
	}
	defaultPullTime = 60 * time.Second
)

func Test_CAS_ErrorNoRetry(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	expectedErr := errors.Errorf("test")

	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Twice()
	descMock.On("Clone").Return(descMock).Once()

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return nil, false, expectedErr
	})

	require.Equal(t, err, expectedErr)
}

func Test_CAS_Backoff(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	expectedErr := errors.Errorf("test")

	ddbMock.On("Query").Return(map[string][]byte{}, expectedErr).Once()
	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Twice()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, []string{}, nil).Once()
	codecMock.On("EncodeMultiKey").Return(map[string][]byte{}, nil).Twice()

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return descMock, true, nil
	})

	require.NoError(t, err)
}

func Test_CAS_Failed(t *testing.T) {
	config := backoff.Config{
		MinBackoff: 1 * time.Millisecond,
		MaxBackoff: 1 * time.Millisecond,
		MaxRetries: 10,
	}
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, config)

	ddbMock.On("Query").Return(map[string][]byte{}, errors.Errorf("test"))

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return descMock, true, nil
	})

	ddbMock.AssertNumberOfCalls(t, "Query", 10)
	require.Error(t, err, "failed to CAS")
}

func Test_CAS_Update(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	expectedUpdatedKeys := []string{"t1", "t2"}
	expectedUpdated := map[string][]byte{
		expectedUpdatedKeys[0]: []byte(expectedUpdatedKeys[0]),
		expectedUpdatedKeys[1]: []byte(expectedUpdatedKeys[1]),
	}
	expectedBatch := map[dynamodbKey][]byte{
		{primaryKey: key, sortKey: expectedUpdatedKeys[0]}: []byte(expectedUpdatedKeys[0]),
		{primaryKey: key, sortKey: expectedUpdatedKeys[1]}: []byte(expectedUpdatedKeys[1]),
	}

	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Once()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, []string{}, nil).Once()
	codecMock.On("EncodeMultiKey").Return(expectedUpdated, nil).Once()
	ddbMock.On("Batch", context.TODO(), expectedBatch, []dynamodbKey{}).Once()

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return descMock, true, nil
	})

	require.NoError(t, err)
	ddbMock.AssertNumberOfCalls(t, "Batch", 1)
	ddbMock.AssertCalled(t, "Batch", context.TODO(), expectedBatch, []dynamodbKey{})
}

func Test_CAS_Delete(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	expectedToDelete := []string{"test", "test2"}
	expectedBatch := []dynamodbKey{
		{primaryKey: key, sortKey: expectedToDelete[0]},
		{primaryKey: key, sortKey: expectedToDelete[1]},
	}

	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Once()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, expectedToDelete, nil).Once()
	codecMock.On("EncodeMultiKey").Return(map[string][]byte{}, nil).Once()
	ddbMock.On("Batch", context.TODO(), map[dynamodbKey][]byte{}, expectedBatch)

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return descMock, true, nil
	})

	require.NoError(t, err)
	ddbMock.AssertNumberOfCalls(t, "Batch", 1)
	ddbMock.AssertCalled(t, "Batch", context.TODO(), map[dynamodbKey][]byte{}, expectedBatch)
}

func Test_CAS_Update_Delete(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	expectedUpdatedKeys := []string{"t1", "t2"}
	expectedUpdated := map[string][]byte{
		expectedUpdatedKeys[0]: []byte(expectedUpdatedKeys[0]),
		expectedUpdatedKeys[1]: []byte(expectedUpdatedKeys[1]),
	}
	expectedUpdateBatch := map[dynamodbKey][]byte{
		{primaryKey: key, sortKey: expectedUpdatedKeys[0]}: []byte(expectedUpdatedKeys[0]),
		{primaryKey: key, sortKey: expectedUpdatedKeys[1]}: []byte(expectedUpdatedKeys[1]),
	}
	expectedToDelete := []string{"test", "test2"}
	expectedDeleteBatch := []dynamodbKey{
		{primaryKey: key, sortKey: expectedToDelete[0]},
		{primaryKey: key, sortKey: expectedToDelete[1]},
	}

	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Once()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, expectedToDelete, nil).Once()
	codecMock.On("EncodeMultiKey").Return(expectedUpdated, nil).Once()
	ddbMock.On("Batch", context.TODO(), expectedUpdateBatch, expectedDeleteBatch)

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return descMock, true, nil
	})

	require.NoError(t, err)
	ddbMock.AssertNumberOfCalls(t, "Batch", 1)
	ddbMock.AssertCalled(t, "Batch", context.TODO(), expectedUpdateBatch, expectedDeleteBatch)
}

func Test_WatchKey(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), 1*time.Second, defaultBackoff)
	timesCalled := 0

	ddbMock.On("Query").Return(map[string][]byte{}, nil)
	codecMock.On("DecodeMultiKey").Return(descMock, nil)

	c.WatchKey(context.TODO(), key, func(i interface{}) bool {
		timesCalled++
		ddbMock.AssertNumberOfCalls(t, "Query", timesCalled)
		codecMock.AssertNumberOfCalls(t, "DecodeMultiKey", timesCalled)
		require.EqualValues(t, descMock, i)
		return timesCalled < 5
	})
}

func Test_WatchKey_UpdateStale(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	staleData := &DescMock{}

	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(staleData, nil)

	c.WatchKey(context.TODO(), key, func(i interface{}) bool {
		ddbMock.AssertNumberOfCalls(t, "Query", 1)
		codecMock.AssertNumberOfCalls(t, "DecodeMultiKey", 1)
		require.EqualValues(t, staleData, i)
		return false
	})

	ddbMock.On("Query").Return(map[string][]byte{}, errors.Errorf("failed"))
	staleData.On("Clone").Return(staleData).Once()

	c.WatchKey(context.TODO(), key, func(i interface{}) bool {
		ddbMock.AssertNumberOfCalls(t, "Query", 12)
		codecMock.AssertNumberOfCalls(t, "DecodeMultiKey", 1)
		require.EqualValues(t, staleData, i)
		return false
	})
}

func Test_WatchPrefix(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	data := map[string][]byte{}
	dataKey := []string{"t1", "t2"}
	data[dataKey[0]] = []byte(dataKey[0])
	data[dataKey[1]] = []byte(dataKey[1])
	calls := 0

	ddbMock.On("Query").Return(data, nil)
	codecMock.On("Decode").Twice()

	c.WatchPrefix(context.TODO(), key, func(key string, i interface{}) bool {
		require.EqualValues(t, string(data[key]), i)
		delete(data, key)
		calls++
		return calls < 2
	})

	require.True(t, len(data) == 0)

	ddbMock.AssertNumberOfCalls(t, "Query", 1)
}

func Test_UpdateStaleData(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	staleData := &DescMock{}
	timestamp := time.Date(2000, 10, 10, 10, 10, 10, 10, time.UTC)

	c.updateStaleData(key, staleData, timestamp)

	require.NotNil(t, c.staleData[key])
	require.EqualValues(t, c.staleData[key].data, staleData)
	require.EqualValues(t, c.staleData[key].timestamp, timestamp)

}

// NewClientMock makes a new local dynamodb client.
func NewClientMock(ddbClient dynamoDbClient, cc codec.Codec, logger log.Logger, registerer prometheus.Registerer, time time.Duration, config backoff.Config) *Client {
	return &Client{
		kv:             ddbClient,
		ddbMetrics:     newDynamoDbMetrics(registerer),
		codec:          cc,
		logger:         logger,
		staleData:      make(map[string]staleData),
		pullerSyncTime: time,
		backoffConfig:  config,
	}

}

type MockDynamodbClient struct {
	mock.Mock
}

//revive:disable:unexported-return
func NewDynamodbClientMock() *MockDynamodbClient {
	return &MockDynamodbClient{}
}

//revive:enable:unexported-return

func (m *MockDynamodbClient) List(context.Context, dynamodbKey) ([]string, float64, error) {
	args := m.Called()
	var err error
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return args.Get(0).([]string), 0, err
}
func (m *MockDynamodbClient) Query(context.Context, dynamodbKey, bool) (map[string][]byte, float64, error) {
	args := m.Called()
	var err error
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return args.Get(0).(map[string][]byte), 0, err
}
func (m *MockDynamodbClient) Delete(ctx context.Context, key dynamodbKey) error {
	m.Called(ctx, key)
	return nil
}
func (m *MockDynamodbClient) Put(ctx context.Context, key dynamodbKey, data []byte) error {
	m.Called(ctx, key, data)
	return nil
}
func (m *MockDynamodbClient) Batch(ctx context.Context, put map[dynamodbKey][]byte, delete []dynamodbKey) error {
	m.Called(ctx, put, delete)
	return nil
}

type TestLogger struct {
}

func (l TestLogger) Log(...interface{}) error {
	return nil
}

// String is a code for strings.
type CodecMock struct {
	mock.Mock
}

func (*CodecMock) CodecID() string {
	return "CodecMock"
}

// Decode implements Codec.
func (m *CodecMock) Decode(bytes []byte) (interface{}, error) {
	m.Called()
	return string(bytes), nil
}

// Encode implements Codec.
func (m *CodecMock) Encode(i interface{}) ([]byte, error) {
	m.Called()
	return []byte(i.(string)), nil
}

func (m *CodecMock) EncodeMultiKey(interface{}) (map[string][]byte, error) {
	args := m.Called()
	return args.Get(0).(map[string][]byte), nil
}

func (m *CodecMock) DecodeMultiKey(map[string][]byte) (interface{}, error) {
	args := m.Called()
	var err error
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return args.Get(0), err
}

type DescMock struct {
	mock.Mock
}

func (m *DescMock) Clone() interface{} {
	args := m.Called()
	return args.Get(0)
}

func (m *DescMock) SplitByID() map[string]interface{} {
	args := m.Called()
	return args.Get(0).(map[string]interface{})
}

func (m *DescMock) JoinIds(map[string]interface{}) {
	m.Called()
}

func (m *DescMock) GetItemFactory() proto.Message {
	args := m.Called()
	return args.Get(0).(proto.Message)
}

func (m *DescMock) FindDifference(that codec.MultiKey) (interface{}, []string, error) {
	args := m.Called(that)
	var err error
	if args.Get(2) != nil {
		err = args.Get(2).(error)
	}
	return args.Get(0), args.Get(1).([]string), err
}
