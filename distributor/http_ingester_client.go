package distributor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/user"
)

// IngesterClient is a client library for the ingester
type httpIngesterClient struct {
	address string
	client  http.Client
	timeout time.Duration
}

// IngesterError is an error we got from an ingester.
type IngesterError struct {
	StatusCode int
	err        error
}

func errStatusCode(code int, status string) IngesterError {
	return IngesterError{
		StatusCode: code,
		err:        fmt.Errorf("server returned HTTP status %s", status),
	}
}

func (i IngesterError) Error() string {
	return i.err.Error()
}

// NewHTTPIngesterClient makes a new IngesterClient.  This client is careful to
// propagate the user ID from Distributor -> Ingester.
func NewHTTPIngesterClient(address string, timeout time.Duration) (cortex.IngesterClient, error) {
	return &httpIngesterClient{
		address: address,
		client: http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
	}, nil
}

// Push adds new samples to the ingester
func (c *httpIngesterClient) Push(ctx context.Context, req *remote.WriteRequest, _ ...grpc.CallOption) (*cortex.WriteResponse, error) {
	if err := c.doRequest(ctx, "/push", req, nil, true); err != nil {
		return nil, err
	}
	return &cortex.WriteResponse{}, nil
}

func (c *httpIngesterClient) Query(ctx context.Context, req *cortex.QueryRequest, _ ...grpc.CallOption) (*cortex.QueryResponse, error) {
	resp := &cortex.QueryResponse{}
	if err := c.doRequest(ctx, "/query", req, resp, false); err != nil {
		return nil, err
	}
	return resp, nil
}

// LabelValues returns all of the label values that are associated with a given label name.
func (c *httpIngesterClient) LabelValues(ctx context.Context, req *cortex.LabelValuesRequest, _ ...grpc.CallOption) (*cortex.LabelValuesResponse, error) {
	resp := &cortex.LabelValuesResponse{}
	err := c.doRequest(ctx, "/label_values", req, resp, false)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UserStats returns stats for the current user.
func (c *httpIngesterClient) UserStats(ctx context.Context, in *cortex.UserStatsRequest, _ ...grpc.CallOption) (*cortex.UserStatsResponse, error) {
	resp := &cortex.UserStatsResponse{}
	err := c.doRequest(ctx, "/user_stats", nil, resp, false)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *httpIngesterClient) doRequest(ctx context.Context, endpoint string, req proto.Message, resp proto.Message, compressed bool) error {
	userID, err := user.GetID(ctx)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if req != nil {
		data, err := proto.Marshal(req)
		if err != nil {
			return fmt.Errorf("unable to marshal request: %v", err)
		}

		var writer io.Writer = &buf
		if compressed {
			writer = snappy.NewWriter(writer)
		}
		if _, err := writer.Write(data); err != nil {
			return err
		}
	}

	httpReq, err := http.NewRequest("POST", fmt.Sprintf("http://%s%s", c.address, endpoint), &buf)
	if err != nil {
		return fmt.Errorf("unable to create request: %v", err)
	}
	httpReq.Header.Add(user.UserIDHeaderName, userID)
	// TODO: This isn't actually the correct Content-type.
	httpReq.Header.Set("Content-Type", string(expfmt.FmtProtoDelim))
	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode/100 != 2 {
		return errStatusCode(httpResp.StatusCode, httpResp.Status)
	}
	if resp == nil {
		return nil
	}

	buf.Reset()
	reader := httpResp.Body.(io.Reader)
	if compressed {
		reader = snappy.NewReader(reader)
	}
	if _, err = buf.ReadFrom(reader); err != nil {
		return fmt.Errorf("unable to read response body: %v", err)
	}

	err = proto.Unmarshal(buf.Bytes(), resp)
	if err != nil {
		return fmt.Errorf("unable to unmarshal response body: %v", err)
	}
	return nil
}
