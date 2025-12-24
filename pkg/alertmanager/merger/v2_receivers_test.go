package merger

import (
	"testing"

	"github.com/go-openapi/swag/jsonutils"
	v2_models "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/require"
)

// stringPtr is a helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

func TestV2ReceiversMerge(t *testing.T) {
	in1 := []*v2_models.Receiver{
		{Name: stringPtr("receiver1")},
		{Name: stringPtr("receiver2")},
	}
	in2 := []*v2_models.Receiver{
		{Name: stringPtr("receiver2")}, // Duplicate
		{Name: stringPtr("receiver3")},
	}
	in3 := []*v2_models.Receiver{
		{Name: stringPtr("receiver1")}, // Duplicate
		{Name: stringPtr("receiver4")},
	}

	body1, err := jsonutils.WriteJSON(in1)
	require.NoError(t, err)
	body2, err := jsonutils.WriteJSON(in2)
	require.NoError(t, err)
	body3, err := jsonutils.WriteJSON(in3)
	require.NoError(t, err)

	merger := V2Receivers{}
	out, err := merger.MergeResponses([][]byte{body1, body2, body3})
	require.NoError(t, err)

	var result []*v2_models.Receiver
	err = jsonutils.ReadJSON(out, &result)
	require.NoError(t, err)

	// Should have 4 unique receivers
	require.Len(t, result, 4)

	// Check that all expected receivers are present
	names := make(map[string]bool)
	for _, r := range result {
		require.NotNil(t, r.Name)
		names[*r.Name] = true
	}

	require.True(t, names["receiver1"])
	require.True(t, names["receiver2"])
	require.True(t, names["receiver3"])
	require.True(t, names["receiver4"])
}

func TestV2ReceiversMergeEmpty(t *testing.T) {
	merger := V2Receivers{}
	out, err := merger.MergeResponses([][]byte{})
	require.NoError(t, err)

	var result []*v2_models.Receiver
	err = jsonutils.ReadJSON(out, &result)
	require.NoError(t, err)
	require.Len(t, result, 0)
}

func TestV2ReceiversMergeSingleResponse(t *testing.T) {
	in := []*v2_models.Receiver{
		{Name: stringPtr("receiver1")},
		{Name: stringPtr("receiver2")},
	}

	body, err := jsonutils.WriteJSON(in)
	require.NoError(t, err)

	merger := V2Receivers{}
	out, err := merger.MergeResponses([][]byte{body})
	require.NoError(t, err)

	var result []*v2_models.Receiver
	err = jsonutils.ReadJSON(out, &result)
	require.NoError(t, err)
	require.Len(t, result, 2)
}
