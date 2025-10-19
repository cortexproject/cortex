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

	ddbMock.On("Query").Return(map[string]dynamodbItem{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Twice()
	descMock.On("Clone").Return(descMock).Once()

	err := c.CAS(context.TODO(), key, func(in any) (out any, retry bool, err error) {
		return nil, false, expectedErr
	})

	require.Equal(t, err, expectedErr)
}

func Test_CAS_Backoff(t *testing.T) {
	testCases := []struct {
		name               string
		setupMocks         func(*MockDynamodbClient, *CodecMock, *DescMock, map[dynamodbKey]dynamodbItem, []dynamodbKey)
		expectedQueryCalls int
		expectedBatchCalls int
	}{
		{
			name: "query_fails_and_backs_off",
			setupMocks: func(ddbMock *MockDynamodbClient, codecMock *CodecMock, descMock *DescMock, expectedBatch map[dynamodbKey]dynamodbItem, expectedDelete []dynamodbKey) {
				ddbMock.On("Query").Return(map[string]dynamodbItem{}, errors.Errorf("query failed")).Once()
				ddbMock.On("Query").Return(map[string]dynamodbItem{}, nil).Once()
				ddbMock.On("Batch", context.TODO(), expectedBatch, expectedDelete).Return(false, nil).Once()
			},
			expectedQueryCalls: 2,
			expectedBatchCalls: 1,
		},
		{
			name: "batch_fails_and_backs_off",
			setupMocks: func(ddbMock *MockDynamodbClient, codecMock *CodecMock, descMock *DescMock, expectedBatch map[dynamodbKey]dynamodbItem, expectedDelete []dynamodbKey) {
				ddbMock.On("Query").Return(map[string]dynamodbItem{}, nil).Twice()
				ddbMock.On("Batch", context.TODO(), expectedBatch, expectedDelete).Return(true, errors.Errorf("batch failed")).Once()
				ddbMock.On("Batch", context.TODO(), expectedBatch, expectedDelete).Return(false, nil).Once()
			},
			expectedQueryCalls: 2,
			expectedBatchCalls: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ddbMock := NewDynamodbClientMock()
			codecMock := &CodecMock{}
			descMock := &DescMock{}
			c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)

			expectedBatch := map[dynamodbKey]dynamodbItem{}
			expectedDelete := []dynamodbKey{{primaryKey: "test", sortKey: "childkey"}}

			tc.setupMocks(ddbMock, codecMock, descMock, expectedBatch, expectedDelete)

			codecMock.On("DecodeMultiKey").Return(descMock, nil).Times(tc.expectedQueryCalls)
			descMock.On("Clone").Return(descMock).Times(tc.expectedQueryCalls)
			descMock.On("FindDifference", descMock).Return(descMock, []string{"childkey"}, nil).Times(tc.expectedBatchCalls)
			codecMock.On("EncodeMultiKey").Return(map[string][]byte{}, nil).Times(tc.expectedBatchCalls)

			err := c.CAS(context.TODO(), key, func(in any) (out any, retry bool, err error) {
				return descMock, true, nil
			})

			require.NoError(t, err)
			ddbMock.AssertNumberOfCalls(t, "Query", tc.expectedQueryCalls)
			ddbMock.AssertNumberOfCalls(t, "Batch", tc.expectedBatchCalls)
		})
	}
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

	ddbMock.On("Query").Return(map[string]dynamodbItem{}, errors.Errorf("test"))

	err := c.CAS(context.TODO(), key, func(in any) (out any, retry bool, err error) {
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
	expectedBatch := map[dynamodbKey]dynamodbItem{
		{primaryKey: key, sortKey: expectedUpdatedKeys[0]}: {data: []byte(expectedUpdatedKeys[0])},
		{primaryKey: key, sortKey: expectedUpdatedKeys[1]}: {data: []byte(expectedUpdatedKeys[1])},
	}

	ddbMock.On("Query").Return(map[string]dynamodbItem{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Once()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, []string{}, nil).Once()
	codecMock.On("EncodeMultiKey").Return(expectedUpdated, nil).Once()
	ddbMock.On("Batch", context.TODO(), expectedBatch, []dynamodbKey{}).Return(false, nil).Once()

	err := c.CAS(context.TODO(), key, func(in any) (out any, retry bool, err error) {
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

	ddbMock.On("Query").Return(map[string]dynamodbItem{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Once()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, expectedToDelete, nil).Once()
	codecMock.On("EncodeMultiKey").Return(map[string][]byte{}, nil).Once()
	ddbMock.On("Batch", context.TODO(), map[dynamodbKey]dynamodbItem{}, expectedBatch).Return(false, nil).Once()

	err := c.CAS(context.TODO(), key, func(in any) (out any, retry bool, err error) {
		return descMock, true, nil
	})

	require.NoError(t, err)
	ddbMock.AssertNumberOfCalls(t, "Batch", 1)
	ddbMock.AssertCalled(t, "Batch", context.TODO(), map[dynamodbKey]dynamodbItem{}, expectedBatch)
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
	expectedUpdateBatch := map[dynamodbKey]dynamodbItem{
		{primaryKey: key, sortKey: expectedUpdatedKeys[0]}: {data: []byte(expectedUpdatedKeys[0])},
		{primaryKey: key, sortKey: expectedUpdatedKeys[1]}: {data: []byte(expectedUpdatedKeys[1])},
	}
	expectedToDelete := []string{"test", "test2"}
	expectedDeleteBatch := []dynamodbKey{
		{primaryKey: key, sortKey: expectedToDelete[0]},
		{primaryKey: key, sortKey: expectedToDelete[1]},
	}

	ddbMock.On("Query").Return(map[string]dynamodbItem{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Once()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, expectedToDelete, nil).Once()
	codecMock.On("EncodeMultiKey").Return(expectedUpdated, nil).Once()
	ddbMock.On("Batch", context.TODO(), expectedUpdateBatch, expectedDeleteBatch).Return(false, nil).Once()

	err := c.CAS(context.TODO(), key, func(in any) (out any, retry bool, err error) {
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

	ddbMock.On("Query").Return(map[string]dynamodbItem{}, nil)
	codecMock.On("DecodeMultiKey").Return(descMock, nil)

	c.WatchKey(context.TODO(), key, func(i any) bool {
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

	ddbMock.On("Query").Return(map[string]dynamodbItem{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(staleData, nil)

	c.WatchKey(context.TODO(), key, func(i any) bool {
		ddbMock.AssertNumberOfCalls(t, "Query", 1)
		codecMock.AssertNumberOfCalls(t, "DecodeMultiKey", 1)
		require.EqualValues(t, staleData, i)
		return false
	})

	ddbMock.On("Query").Return(map[string]dynamodbItem{}, errors.Errorf("failed"))
	staleData.On("Clone").Return(staleData).Once()

	c.WatchKey(context.TODO(), key, func(i any) bool {
		ddbMock.AssertNumberOfCalls(t, "Query", 12)
		codecMock.AssertNumberOfCalls(t, "DecodeMultiKey", 1)
		require.EqualValues(t, staleData, i)
		return false
	})
}

func Test_CAS_UpdateStale(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	descMockResult := &DescMock{}
	startTime := time.Now().UTC().Add(-time.Millisecond)

	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	expectedUpdatedKeys := []string{"t1", "t2"}
	expectedUpdated := map[string][]byte{
		expectedUpdatedKeys[0]: []byte(expectedUpdatedKeys[0]),
		expectedUpdatedKeys[1]: []byte(expectedUpdatedKeys[1]),
	}
	expectedBatch := map[dynamodbKey]dynamodbItem{
		{primaryKey: key, sortKey: expectedUpdatedKeys[0]}: {data: []byte(expectedUpdatedKeys[0])},
		{primaryKey: key, sortKey: expectedUpdatedKeys[1]}: {data: []byte(expectedUpdatedKeys[1])},
	}

	ddbMock.On("Query").Return(map[string]dynamodbItem{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Once()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMockResult).Return(descMockResult, []string{}, nil).Once()
	codecMock.On("EncodeMultiKey").Return(expectedUpdated, nil).Once()
	ddbMock.On("Batch", context.TODO(), expectedBatch, []dynamodbKey{}).Return(false, nil).Once()

	err := c.CAS(context.TODO(), key, func(in any) (out any, retry bool, err error) {
		return descMockResult, true, nil
	})

	require.NoError(t, err)
	require.Equal(t, descMockResult, c.staleData[key].data)
	require.True(t, startTime.Before(c.staleData[key].timestamp))
}

func Test_WatchPrefix(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	c := NewClientMock(ddbMock, codecMock, TestLogger{}, prometheus.NewPedanticRegistry(), defaultPullTime, defaultBackoff)
	data := map[string]dynamodbItem{}
	dataKey := []string{"t1", "t2"}
	data[dataKey[0]] = dynamodbItem{data: []byte(dataKey[0])}
	data[dataKey[1]] = dynamodbItem{data: []byte(dataKey[1])}
	calls := 0

	ddbMock.On("Query").Return(data, nil)
	codecMock.On("Decode").Twice()

	c.WatchPrefix(context.TODO(), key, func(key string, i any) bool {
		require.EqualValues(t, string(data[key].data), i)
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

func Test_DynamodbKVWithTimeout(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	// Backend has delay of 5s while the client timeout is 1s.
	ddbWithDelay := newDynamodbKVWithDelay(ddbMock, time.Second*5)
	dbWithTimeout := newDynamodbKVWithTimeout(ddbWithDelay, time.Second)

	ctx := context.Background()
	_, _, err := dbWithTimeout.List(ctx, dynamodbKey{primaryKey: key})
	require.True(t, errors.Is(err, context.DeadlineExceeded))

	err = dbWithTimeout.Delete(ctx, dynamodbKey{primaryKey: key})
	require.True(t, errors.Is(err, context.DeadlineExceeded))

	_, _, err = dbWithTimeout.Query(ctx, dynamodbKey{primaryKey: key}, true)
	require.True(t, errors.Is(err, context.DeadlineExceeded))

	err = dbWithTimeout.Put(ctx, dynamodbKey{primaryKey: key}, []byte{})
	require.True(t, errors.Is(err, context.DeadlineExceeded))

	_, err = dbWithTimeout.Batch(ctx, nil, nil)
	require.True(t, errors.Is(err, context.DeadlineExceeded))
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
func (m *MockDynamodbClient) Query(context.Context, dynamodbKey, bool) (map[string]dynamodbItem, float64, error) {
	args := m.Called()
	var err error
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return args.Get(0).(map[string]dynamodbItem), 0, err
}
func (m *MockDynamodbClient) Delete(ctx context.Context, key dynamodbKey) error {
	m.Called(ctx, key)
	return nil
}
func (m *MockDynamodbClient) Put(ctx context.Context, key dynamodbKey, data []byte) error {
	m.Called(ctx, key, data)
	return nil
}
func (m *MockDynamodbClient) Batch(ctx context.Context, put map[dynamodbKey]dynamodbItem, delete []dynamodbKey) (bool, error) {
	args := m.Called(ctx, put, delete)
	var err error
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return args.Get(0).(bool), err
}

type TestLogger struct {
}

func (l TestLogger) Log(...any) error {
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
func (m *CodecMock) Decode(bytes []byte) (any, error) {
	m.Called()
	return string(bytes), nil
}

// Encode implements Codec.
func (m *CodecMock) Encode(i any) ([]byte, error) {
	m.Called()
	return []byte(i.(string)), nil
}

func (m *CodecMock) EncodeMultiKey(any) (map[string][]byte, error) {
	args := m.Called()
	return args.Get(0).(map[string][]byte), nil
}

func (m *CodecMock) DecodeMultiKey(map[string][]byte) (any, error) {
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

func (m *DescMock) Clone() any {
	args := m.Called()
	return args.Get(0)
}

func (m *DescMock) SplitByID() map[string]any {
	args := m.Called()
	return args.Get(0).(map[string]any)
}

func (m *DescMock) JoinIds(map[string]any) {
	m.Called()
}

func (m *DescMock) GetItemFactory() proto.Message {
	args := m.Called()
	return args.Get(0).(proto.Message)
}

func (m *DescMock) FindDifference(that codec.MultiKey) (any, []string, error) {
	args := m.Called(that)
	var err error
	if args.Get(2) != nil {
		err = args.Get(2).(error)
	}
	return args.Get(0), args.Get(1).([]string), err
}

type dynamodbKVWithDelayAndContextCheck struct {
	ddbClient dynamoDbClient
	delay     time.Duration
}

func newDynamodbKVWithDelay(client dynamoDbClient, delay time.Duration) *dynamodbKVWithDelayAndContextCheck {
	return &dynamodbKVWithDelayAndContextCheck{ddbClient: client, delay: delay}
}

func (d *dynamodbKVWithDelayAndContextCheck) List(ctx context.Context, key dynamodbKey) ([]string, float64, error) {
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case <-time.After(d.delay):
		return d.ddbClient.List(ctx, key)
	}
}

func (d *dynamodbKVWithDelayAndContextCheck) Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string]dynamodbItem, float64, error) {
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case <-time.After(d.delay):
		return d.ddbClient.Query(ctx, key, isPrefix)
	}
}

func (d *dynamodbKVWithDelayAndContextCheck) Delete(ctx context.Context, key dynamodbKey) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d.delay):
		return d.ddbClient.Delete(ctx, key)
	}
}

func (d *dynamodbKVWithDelayAndContextCheck) Put(ctx context.Context, key dynamodbKey, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d.delay):
		return d.ddbClient.Put(ctx, key, data)
	}
}

func (d *dynamodbKVWithDelayAndContextCheck) Batch(ctx context.Context, put map[dynamodbKey]dynamodbItem, delete []dynamodbKey) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-time.After(d.delay):
		return d.ddbClient.Batch(ctx, put, delete)
	}
}
