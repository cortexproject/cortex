package client

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type IngesterServerMock struct {
	mock.Mock
}

func (m *IngesterServerMock) Push(ctx context.Context, r *WriteRequest) (*WriteResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*WriteResponse), args.Error(1)
}

func (m *IngesterServerMock) Query(ctx context.Context, r *QueryRequest) (*QueryResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*QueryResponse), args.Error(1)
}

func (m *IngesterServerMock) QueryStream(r *QueryRequest, s Ingester_QueryStreamServer) error {
	args := m.Called(r, s)
	return args.Error(0)
}

func (m *IngesterServerMock) LabelValues(ctx context.Context, r *LabelValuesRequest) (*LabelValuesResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*LabelValuesResponse), args.Error(1)
}

func (m *IngesterServerMock) LabelNames(ctx context.Context, r *LabelNamesRequest) (*LabelNamesResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*LabelNamesResponse), args.Error(1)
}

func (m *IngesterServerMock) UserStats(ctx context.Context, r *UserStatsRequest) (*UserStatsResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*UserStatsResponse), args.Error(1)
}

func (m *IngesterServerMock) AllUserStats(ctx context.Context, r *UserStatsRequest) (*UsersStatsResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*UsersStatsResponse), args.Error(1)
}

func (m *IngesterServerMock) MetricsForLabelMatchers(ctx context.Context, r *MetricsForLabelMatchersRequest) (*MetricsForLabelMatchersResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*MetricsForLabelMatchersResponse), args.Error(1)
}

func (m *IngesterServerMock) MetricsMetadata(ctx context.Context, r *MetricsMetadataRequest) (*MetricsMetadataResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*MetricsMetadataResponse), args.Error(1)
}

func (m *IngesterServerMock) TransferChunks(s Ingester_TransferChunksServer) error {
	args := m.Called(s)
	return args.Error(0)
}
