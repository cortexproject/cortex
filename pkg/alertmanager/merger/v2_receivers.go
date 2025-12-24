package merger

import (
	"github.com/go-openapi/swag/jsonutils"
	v2_models "github.com/prometheus/alertmanager/api/v2/models"
)

// V2Receivers implements the Merger interface for GET /v2/receivers. It returns the union of receivers
// over all the responses. When a receiver with the same name exists in multiple responses, any one
// of them is returned (they should be identical across replicas).
type V2Receivers struct{}

func (V2Receivers) MergeResponses(in [][]byte) ([]byte, error) {
	receivers := make([]*v2_models.Receiver, 0)
	for _, body := range in {
		parsed := make([]*v2_models.Receiver, 0)
		if err := jsonutils.ReadJSON(body, &parsed); err != nil {
			return nil, err
		}
		receivers = append(receivers, parsed...)
	}

	merged := mergeV2Receivers(receivers)
	return jsonutils.WriteJSON(merged)
}

func mergeV2Receivers(in []*v2_models.Receiver) []*v2_models.Receiver {
	// Deduplicate receivers by name. Since receivers should be identical across replicas
	// (they come from the same config), we just keep the first occurrence of each name.
	seen := make(map[string]bool)
	result := make([]*v2_models.Receiver, 0)

	for _, receiver := range in {
		if receiver.Name == nil {
			continue
		}

		name := *receiver.Name
		if !seen[name] {
			seen[name] = true
			result = append(result, receiver)
		}
	}

	return result
}
