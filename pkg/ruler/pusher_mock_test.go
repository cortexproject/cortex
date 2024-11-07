package ruler

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/cortexpbv2"
)

type pusherMock struct {
	mock.Mock
}

func newPusherMock() *pusherMock {
	return &pusherMock{}
}

func (m *pusherMock) PushV2(ctx context.Context, req *cortexpbv2.WriteRequest) (*cortexpbv2.WriteResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*cortexpbv2.WriteResponse), args.Error(1)
}

func (m *pusherMock) Push(ctx context.Context, req *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*cortexpb.WriteResponse), args.Error(1)
}

func (m *pusherMock) MockPush(res *cortexpb.WriteResponse, err error) {
	m.On("Push", mock.Anything, mock.Anything).Return(res, err)
}
