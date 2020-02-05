package ruler

import (
	"context"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/stretchr/testify/mock"
)

type pusherMock struct {
	mock.Mock
}

func newPusherMock() *pusherMock {
	return &pusherMock{}
}

func (m *pusherMock) Push(ctx context.Context, req *client.WriteRequest) (*client.WriteResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*client.WriteResponse), args.Error(1)
}

func (m *pusherMock) MockPush(res *client.WriteResponse, err error) {
	m.On("Push", mock.Anything, mock.Anything).Return(res, err)
}
