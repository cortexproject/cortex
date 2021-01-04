package aws

import "github.com/pkg/errors"

const (
	// SSEKMS config type constant to configure S3 server side encryption using KMS
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
	SSEKMS = "SSE-KMS"
	// SSES3 config type constant to configure S3 server side encryption with AES-256
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
	SSES3 = "SSE-S3"
)

// SSEEncryptionConfig configures server side encryption (SSE)
type SSEEncryptionConfig struct {
	ServerSideEncryption string
	KMSKeyID             string
}

// NewSSEEncryptionConfig creates a struct to configure server side encryption (SSE)
func NewSSEEncryptionConfig(sseType, kmsKeyID string) (*SSEEncryptionConfig, error) {
	switch sseType {
	case SSES3:
		return &SSEEncryptionConfig{ServerSideEncryption: "AES256"}, nil
	case SSEKMS:
		if kmsKeyID == "" {
			return nil, errors.New("kms key must be presented when aws:kms SSE encryption is selected")
		}
		return &SSEEncryptionConfig{
			ServerSideEncryption: "aws:kms",
			KMSKeyID:             kmsKeyID,
		}, nil
	default:
		return nil, errors.New("SSE encryption is empty or invalid")
	}
}
