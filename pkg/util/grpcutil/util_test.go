package grpcutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/util/requestmeta"
)

// TestExtractForwardedRequestMetadataFromMetadata tests the extractForwardedRequestMetadataFromMetadata function
func TestExtractForwardedRequestMetadataFromMetadata(t *testing.T) {
	tests := []struct {
		name           string
		ctx            context.Context
		expectedResult map[string]string
	}{
		{
			name:           "context without metadata",
			ctx:            context.Background(),
			expectedResult: nil,
		},
		{
			name: "context with new metadata key",
			ctx: func() context.Context {
				md := metadata.New(nil)
				md.Append(requestmeta.PropagationStringForRequestMetadata, "header1", "value1", "header2", "value2")
				return metadata.NewIncomingContext(context.Background(), md)
			}(),
			expectedResult: map[string]string{
				"header1": "value1",
				"header2": "value2",
			},
		},
		{
			name: "context with old metadata key",
			ctx: func() context.Context {
				md := metadata.New(nil)
				md.Append(requestmeta.HeaderPropagationStringForRequestLogging, "header1", "value1", "header2", "value2")
				return metadata.NewIncomingContext(context.Background(), md)
			}(),
			expectedResult: map[string]string{
				"header1": "value1",
				"header2": "value2",
			},
		},
		{
			name: "context with both keys, new key takes precedence",
			ctx: func() context.Context {
				md := metadata.New(nil)
				md.Append(requestmeta.PropagationStringForRequestMetadata, "newheader", "newvalue")
				md.Append(requestmeta.HeaderPropagationStringForRequestLogging, "oldheader", "oldvalue")
				return metadata.NewIncomingContext(context.Background(), md)
			}(),
			expectedResult: map[string]string{
				"newheader": "newvalue",
			},
		},
		{
			name: "context with odd number of metadata values",
			ctx: func() context.Context {
				md := metadata.New(nil)
				md.Append(requestmeta.PropagationStringForRequestMetadata, "header1", "value1", "header2")
				return metadata.NewIncomingContext(context.Background(), md)
			}(),
			expectedResult: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractForwardedRequestMetadataFromMetadata(tt.ctx)
			metadataMap := requestmeta.MapFromContext(result)

			if tt.expectedResult == nil {
				assert.Nil(t, metadataMap)
			} else {
				assert.Equal(t, tt.expectedResult, metadataMap)
			}
		})
	}
}
