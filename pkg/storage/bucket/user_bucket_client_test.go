package bucket

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func TestUserBucketClient_Upload_ShouldInjectCustomSSEConfig(t *testing.T) {
	const (
		kmsKeyID             = "ABC"
		kmsEncryptionContext = "{\"department\":\"10103.0\"}"
	)

	var req *http.Request

	// Start a fake HTTP server which simulate S3.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Keep track of the received request.
		req = r

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s3Cfg := s3.Config{
		Endpoint:        srv.Listener.Addr().String(),
		Region:          "test",
		BucketName:      "test-bucket",
		SecretAccessKey: flagext.Secret{Value: "test"},
		AccessKeyID:     "test",
		Insecure:        true,
	}

	s3Client, err := s3.NewBucketClient(s3Cfg, "test", log.NewNopLogger())
	require.NoError(t, err)

	// Configure the config provider with NO KMS key ID.
	cfgProvider := &mockTenantConfigProvider{}
	userBkt := NewUserBucketClient("user-1", s3Client, cfgProvider)

	err = userBkt.Upload(context.Background(), "test", strings.NewReader("test"))
	require.NoError(t, err)

	// Ensure NO KMS header has been injected.
	assert.Equal(t, "", req.Header.Get("x-amz-server-side-encryption"))
	assert.Equal(t, "", req.Header.Get("x-amz-server-side-encryption-aws-kms-key-id"))
	assert.Equal(t, "", req.Header.Get("x-amz-server-side-encryption-context"))

	// Configure the config provider with a KMS key ID and without encryption context.
	cfgProvider.s3SseType = s3.SSEKMS
	cfgProvider.s3KmsKeyID = kmsKeyID

	err = userBkt.Upload(context.Background(), "test", strings.NewReader("test"))
	require.NoError(t, err)

	// Ensure the KMS header has been injected.
	assert.Equal(t, "aws:kms", req.Header.Get("x-amz-server-side-encryption"))
	assert.Equal(t, kmsKeyID, req.Header.Get("x-amz-server-side-encryption-aws-kms-key-id"))
	assert.Equal(t, "", req.Header.Get("x-amz-server-side-encryption-context"))

	// Configure the config provider with a KMS key ID and encryption context.
	cfgProvider.s3SseType = s3.SSEKMS
	cfgProvider.s3KmsKeyID = kmsKeyID
	cfgProvider.s3KmsEncryptionContext = kmsEncryptionContext

	err = userBkt.Upload(context.Background(), "test", strings.NewReader("test"))
	require.NoError(t, err)

	// Ensure the KMS header has been injected.
	assert.Equal(t, "aws:kms", req.Header.Get("x-amz-server-side-encryption"))
	assert.Equal(t, kmsKeyID, req.Header.Get("x-amz-server-side-encryption-aws-kms-key-id"))
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(kmsEncryptionContext)), req.Header.Get("x-amz-server-side-encryption-context"))
}

type mockTenantConfigProvider struct {
	s3SseType              string
	s3KmsKeyID             string
	s3KmsEncryptionContext string
}

func (m *mockTenantConfigProvider) S3SSEType(_ string) string {
	return m.s3SseType
}

func (m *mockTenantConfigProvider) S3SSEKMSKeyID(_ string) string {
	return m.s3KmsKeyID
}

func (m *mockTenantConfigProvider) S3SSEKMSEncryptionContext(_ string) string {
	return m.s3KmsEncryptionContext
}
