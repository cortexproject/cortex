package ruler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/cortexproject/cortex/pkg/util/api"
)

const (
	statusError = "error"
)

type JsonDecoder struct{}
type Warning []string

func (j JsonDecoder) Decode(body []byte) (promql.Vector, Warning, error) {
	var response api.Response

	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&response); err != nil {
		return nil, nil, err
	}
	if response.Status == statusError {
		return nil, response.Warnings, fmt.Errorf("failed to execute query with error: %s", response.Error)
	}
	data := struct {
		Type   model.ValueType `json:"resultType"`
		Result json.RawMessage `json:"result"`
	}{}

	if responseDataBytes, err := json.Marshal(response.Data); err != nil {
		return nil, response.Warnings, err
	} else {
		if err = json.Unmarshal(responseDataBytes, &data); err != nil {
			return nil, response.Warnings, err
		}
	}

	switch data.Type {
	case model.ValScalar:
		var scalar model.Scalar
		if err := json.Unmarshal(data.Result, &scalar); err != nil {
			return nil, nil, err
		}
		return j.scalarToPromQLVector(scalar), response.Warnings, nil
	case model.ValVector:
		var vector model.Vector
		if err := json.Unmarshal(data.Result, &vector); err != nil {
			return nil, nil, err
		}
		return j.vectorToPromQLVector(vector), response.Warnings, nil
	default:
		return nil, response.Warnings, errors.New("rule result is not a vector or scalar")
	}
}

func (j JsonDecoder) vectorToPromQLVector(vector model.Vector) promql.Vector {
	v := make([]promql.Sample, 0, len(vector))
	for _, sample := range vector {
		metric := make([]labels.Label, 0, len(sample.Metric))
		for k, v := range sample.Metric {
			metric = append(metric, labels.Label{
				Name:  string(k),
				Value: string(v),
			})
		}
		v = append(v, promql.Sample{
			T:      int64(sample.Timestamp),
			F:      float64(sample.Value),
			Metric: metric,
		})
	}
	return v
}

func (j JsonDecoder) scalarToPromQLVector(scalar model.Scalar) promql.Vector {
	return promql.Vector{promql.Sample{
		T:      int64(scalar.Timestamp),
		F:      float64(scalar.Value),
		Metric: labels.Labels{},
	}}
}
