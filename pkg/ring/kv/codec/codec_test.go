package codec

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_EncodeMultikey_CheckInterface(t *testing.T) {
	m := NewProtoCodec("test", newMockMessage)

	_, err := m.EncodeMultiKey(nil)

	require.Error(t, err, "invalid type")
}

func Test_DecodeMultikey_CheckInterface(t *testing.T) {
	m := NewProtoCodec("test", newMockMessage)

	_, err := m.DecodeMultiKey(nil)

	require.Error(t, err, "invalid type")
}

func Test_EncodeMultikey(t *testing.T) {
	codec := NewProtoCodec("test", newProtoDescMock)
	descMock := &DescMock{}
	expectedSplitKeys := []string{"t1", "t2"}
	expectedSplit := map[string]interface{}{
		expectedSplitKeys[0]: descMock,
		expectedSplitKeys[1]: descMock,
	}
	encoded, err := codec.Encode(expectedSplit[expectedSplitKeys[0]])
	require.NoError(t, err)

	descMock.On("SplitByID").Return(expectedSplit)

	out, err := codec.EncodeMultiKey(descMock)

	require.NoError(t, err)
	require.Equal(t, 2, len(out))
	require.Equal(t, encoded, out[expectedSplitKeys[0]])
	require.Equal(t, encoded, out[expectedSplitKeys[1]])
}

func Test_DecodeMultikey(t *testing.T) {
	descMock := &DescMock{}
	codec := NewProtoCodec("test", func() proto.Message {
		return descMock
	})
	mMock := &mockMessage{}
	eT1, err := codec.Encode(mMock)
	require.NoError(t, err)
	eT2, err := codec.Encode(mMock)
	require.NoError(t, err)
	expectedSplit := map[string][]byte{
		"t1": eT1,
		"t2": eT2,
	}

	descMock.On("GetItemFactory").Return(newMockMessage())
	descMock.On("JoinIds").Return(descMock)

	out, err := codec.DecodeMultiKey(expectedSplit)

	require.NoError(t, err)
	descMock.AssertNumberOfCalls(t, "GetItemFactory", 2)
	descMock.AssertNumberOfCalls(t, "JoinIds", 1)
	require.Equal(t, descMock, out)
}

type mockMessage struct {
}

func newMockMessage() proto.Message {
	return &mockMessage{}
}
func (mockMessage) Reset() {}

func (mockMessage) String() string {
	return "mockString"
}

func (mockMessage) ProtoMessage() {
}

type DescMock struct {
	mock.Mock
}

func newProtoDescMock() proto.Message {
	return &DescMock{}
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

func (m *DescMock) FindDifference(that MultiKey) (interface{}, []string, error) {
	args := m.Called(that)
	var err error
	if args.Get(2) != nil {
		err = args.Get(2).(error)
	}
	return args.Get(0), args.Get(1).([]string), err
}

func (m *DescMock) Reset() {}

func (m *DescMock) String() string {
	return "Desc"
}

func (m *DescMock) ProtoMessage() {
}
