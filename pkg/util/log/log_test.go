package log

import (
	"context"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc/metadata"
)

func TestHeaderMapFromMetadata(t *testing.T) {
	md := metadata.New(nil)
	md.Append(HeaderPropagationStringForRequestLogging, "TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2")

	ctx := context.Background()

	ctx = ContextWithHeaderMapFromMetadata(ctx, md)

	headerMap := HeaderMapFromContext(ctx)

	require.Contains(t, headerMap, "TestHeader1")
	require.Contains(t, headerMap, "TestHeader2")
	require.Equal(t, "SomeInformation", headerMap["TestHeader1"])
	require.Equal(t, "ContentsOfTestHeader2", headerMap["TestHeader2"])
}

func TestHeaderMapFromMetadataWithImproperLength(t *testing.T) {
	md := metadata.New(nil)
	md.Append(HeaderPropagationStringForRequestLogging, "TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2", "Test3")

	ctx := context.Background()

	ctx = ContextWithHeaderMapFromMetadata(ctx, md)

	headerMap := HeaderMapFromContext(ctx)
	require.Nil(t, headerMap)
}

func TestInjectHeadersIntoHTTPRequest(t *testing.T) {
	contentsMap := make(map[string]string)
	contentsMap["TestHeader1"] = "RequestID"
	contentsMap["TestHeader2"] = "ContentsOfTestHeader2"

	h := http.Header{}
	req := &http.Request{
		Method:     "GET",
		RequestURI: "/HTTPHeaderTest",
		Body:       http.NoBody,
		Header:     h,
	}
	InjectHeadersIntoHTTPRequest(contentsMap, req)

	header1 := req.Header.Values("TestHeader1")
	header2 := req.Header.Values("TestHeader2")

	require.NotNil(t, header1)
	require.NotNil(t, header2)
	require.Equal(t, 1, len(header1))
	require.Equal(t, 1, len(header2))

	require.Equal(t, "RequestID", header1[0])
	require.Equal(t, "ContentsOfTestHeader2", header2[0])

}

func TestInitLogger(t *testing.T) {
	stderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	defer func() { os.Stderr = stderr }()

	cfg := &server.Config{}
	require.NoError(t, cfg.LogLevel.Set("debug"))
	InitLogger(cfg)

	level.Debug(Logger).Log("hello", "world")
	cfg.Log.Debugf("%s %s", "hello", "world")

	require.NoError(t, w.Close())
	logs, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Contains(t, string(logs), "caller=log_test.go:82 level=debug hello=world")
	require.Contains(t, string(logs), "caller=log_test.go:83 level=debug msg=\"hello world\"")
}

func BenchmarkDisallowedLogLevels(b *testing.B) {
	cfg := &server.Config{}
	require.NoError(b, cfg.LogLevel.Set("warn"))
	InitLogger(cfg)

	for i := 0; i < b.N; i++ {
		level.Info(Logger).Log("hello", "world", "number", i)
		level.Debug(Logger).Log("hello", "world", "number", i)
	}
}
