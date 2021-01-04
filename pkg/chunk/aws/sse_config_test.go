package aws

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestNewSSEEncryptionConfig(t *testing.T) {
	kmsKeyID := "test"
	kmsEncryptionContext := map[string]string{
		"a": "bc",
		"b": "cd",
	}
	parsedKMSEncryptionContext := "eyJhIjoiYmMiLCJiIjoiY2QifQ=="

	type params struct {
		sseType              string
		kmsKeyID             *string
		kmsEncryptionContext map[string]string
	}
	tests := []struct {
		name        string
		params      params
		expected    *SSEEncryptionConfig
		expectedErr error
	}{
		{
			name: "Test SSE encryption with SSES3 type",
			params: params{
				sseType: SSES3,
			},
			expected: &SSEEncryptionConfig{
				ServerSideEncryption: sseS3Type,
			},
		},
		{
			name: "Test SSE encryption with SSEKMS type without context",
			params: params{
				sseType:  SSEKMS,
				kmsKeyID: &kmsKeyID,
			},
			expected: &SSEEncryptionConfig{
				ServerSideEncryption: sseKMSType,
				KMSKeyID:             &kmsKeyID,
			},
		},
		{
			name: "Test SSE encryption with SSEKMS type with context",
			params: params{
				sseType:              SSEKMS,
				kmsKeyID:             &kmsKeyID,
				kmsEncryptionContext: kmsEncryptionContext,
			},
			expected: &SSEEncryptionConfig{
				ServerSideEncryption: sseKMSType,
				KMSKeyID:             &kmsKeyID,
				KMSEncryptionContext: &parsedKMSEncryptionContext,
			},
		},
		{
			name: "Test SSE encryption with SSEKMS type with context",
			params: params{
				sseType:              SSEKMS,
				kmsKeyID:             &kmsKeyID,
				kmsEncryptionContext: kmsEncryptionContext,
			},
			expected: &SSEEncryptionConfig{
				ServerSideEncryption: sseKMSType,
				KMSKeyID:             &kmsKeyID,
				KMSEncryptionContext: &parsedKMSEncryptionContext,
			},
		},
		{
			name: "Test invalid SEE type",
			params: params{
				sseType: "invalid",
			},
			expectedErr: errors.New("SSE type is empty or invalid"),
		},
		{
			name: "Test SSE encryption with SSEKMS type without KMS Key ID",
			params: params{
				sseType:  SSEKMS,
				kmsKeyID: nil,
			},
			expectedErr: errors.New("KMS key id must be passed when SSE-KMS encryption is selected"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewSSEEncryptionConfig(tt.params.sseType, tt.params.kmsKeyID, tt.params.kmsEncryptionContext)
			if tt.expectedErr != nil {
				assert.Equal(t, tt.expectedErr.Error(), err.Error())
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}
