package fakeauth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/util/users"
)

const ignoredMethod = "/test.Service/Ignored"

type fakeServerStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss fakeServerStream) Context() context.Context {
	return ss.ctx
}

func tenantFromUnary(t *testing.T, interceptor grpc.UnaryServerInterceptor, ctx context.Context, method string) (string, error) {
	t.Helper()
	var tenant string
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(ctx context.Context, _ any) (any, error) {
		tenant, _ = users.TenantID(ctx)
		return nil, nil
	})
	return tenant, err
}

func tenantFromStream(t *testing.T, interceptor grpc.StreamServerInterceptor, ctx context.Context, method string) (string, error) {
	t.Helper()
	var tenant string
	err := interceptor(nil, fakeServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: method}, func(_ any, ss grpc.ServerStream) error {
		tenant, _ = users.TenantID(ss.Context())
		return nil
	})
	return tenant, err
}

func TestSetupAuthMiddleware_GRPC(t *testing.T) {
	withTenant := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-scope-orgid", "user-1"))
	withoutTenant := context.Background()

	tests := map[string]struct {
		authEnabled    bool
		ctx            context.Context
		method         string
		expectedTenant string
		expectedCode   codes.Code
	}{
		"auth enabled, tenant in metadata": {
			authEnabled:    true,
			ctx:            withTenant,
			expectedTenant: "user-1",
			expectedCode:   codes.OK,
		},
		"auth enabled, no tenant in metadata": {
			authEnabled:  true,
			ctx:          withoutTenant,
			expectedCode: codes.Unauthenticated,
		},
		"auth enabled, no tenant in metadata, ignored method": {
			authEnabled:  true,
			ctx:          withoutTenant,
			method:       ignoredMethod,
			expectedCode: codes.OK,
		},
		"auth disabled, no tenant in metadata": {
			authEnabled:    false,
			ctx:            withoutTenant,
			expectedTenant: "fake",
			expectedCode:   codes.OK,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := server.Config{}
			SetupAuthMiddleware(&cfg, tc.authEnabled, []string{ignoredMethod})
			require.Len(t, cfg.GRPCMiddleware, 1)
			require.Len(t, cfg.GRPCStreamMiddleware, 1)

			method := tc.method
			if method == "" {
				method = "/test.Service/Method"
			}

			tenant, err := tenantFromUnary(t, cfg.GRPCMiddleware[0], tc.ctx, method)
			assert.Equal(t, tc.expectedCode, status.Code(err), "unary")
			assert.Equal(t, tc.expectedTenant, tenant, "unary")

			tenant, err = tenantFromStream(t, cfg.GRPCStreamMiddleware[0], tc.ctx, method)
			assert.Equal(t, tc.expectedCode, status.Code(err), "stream")
			assert.Equal(t, tc.expectedTenant, tenant, "stream")
		})
	}
}
