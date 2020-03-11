package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	grpc_util "github.com/cortexproject/cortex/pkg/util/grpc"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestSendQueryStream(t *testing.T) {
	// Create a new gRPC server with in-memory communication.
	listen := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	bufDialer := func(context.Context, string) (net.Conn, error) {
		return listen.Dial()
	}

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()

	// Create a cancellable context for the client.
	clientCtx, clientCancel := context.WithCancel(context.Background())

	// Create a WaitGroup used to wait until the mocked server assertions
	// complete before returning.
	wg := sync.WaitGroup{}
	wg.Add(1)

	serverMock := &IngesterServerMock{}
	serverMock.On("QueryStream", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		defer wg.Done()

		stream := args.Get(1).(grpc.ServerStream)

		// Cancel the client request.
		clientCancel()

		// Wait until the cancelling has been propagated to the server.
		test.Poll(t, time.Second, context.Canceled, func() interface{} {
			return stream.Context().Err()
		})

		// Try to send the response and assert the error we get is the context.Canceled
		// and not transport.ErrIllegalHeaderWrite. This is the assertion we care about
		// in this test.
		err := SendQueryStream(stream.(Ingester_QueryStreamServer), &QueryStreamResponse{})
		assert.Equal(t, context.Canceled, err)
	})

	RegisterIngesterServer(server, serverMock)

	go func() {
		require.NoError(t, server.Serve(listen))
	}()

	client := NewIngesterClient(conn)
	stream, err := client.QueryStream(clientCtx, &QueryRequest{})
	require.NoError(t, err)

	// Try to receive the response and assert the error we get is the context.Canceled
	// wrapped within a gRPC error.
	_, err = stream.Recv()
	assert.Equal(t, true, grpc_util.IsGRPCContextCanceled(err))

	// Wait until the assertions in the server mock have completed.
	wg.Wait()
}
