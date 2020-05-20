package querytee

import (
	"encoding/json"
	"fmt"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/cortexproject/cortex/pkg/util"
)

// ResponsesComparatorFunc helps with comparing different responses from various routes.
type ResponsesComparatorFunc func(expected, actual []byte) error

// SamplesComparatorFunc helps with comparing different types of samples coming from /api/v1/query and /api/v1/query_range routes.
type SamplesComparatorFunc func(expected, actual json.RawMessage) error

type SamplesResponse struct {
	Status string
	Data   struct {
		ResultType string
		Result     json.RawMessage
	}
}

var samplesComparator = map[string]SamplesComparatorFunc{
	"matrix": compareMatrix,
	"vector": compareVector,
	"scalar": compareScalar,
}

// RegisterSamplesComparator helps with registering custom sample types
func RegisterSamplesComparator(samplesType string, comparator SamplesComparatorFunc) {
	samplesComparator[samplesType] = comparator
}

func CompareSamplesResponse(expectedResponse, actualResponse []byte) error {
	var expected, actual SamplesResponse

	err := json.Unmarshal(expectedResponse, &expected)
	if err != nil {
		return err
	}

	err = json.Unmarshal(actualResponse, &actual)
	if err != nil {
		return err
	}

	if expected.Status != actual.Status {
		return fmt.Errorf("expected status %s but got %s", expected.Status, actual.Status)
	}

	if expected.Data.ResultType != actual.Data.ResultType {
		return fmt.Errorf("expected resultType %s but got %s", expected.Data.ResultType, actual.Data.ResultType)
	}

	comparator, ok := samplesComparator[expected.Data.ResultType]
	if !ok {
		return fmt.Errorf("resultType %s not registered for comparison", expected.Data.ResultType)
	}

	return comparator(expected.Data.Result, actual.Data.Result)
}

type matrixResult struct {
	Result []struct {
		Metric map[string]string
		Values [][]interface{}
	}
}

func compareMatrix(expectedRaw, actualRaw json.RawMessage) error {
	var expected, actual matrixResult

	err := json.Unmarshal(expectedRaw, &expected.Result)
	if err != nil {
		return err
	}
	err = json.Unmarshal(actualRaw, &actual.Result)
	if err != nil {
		return err
	}

	if len(expected.Result) != len(actual.Result) {
		return fmt.Errorf("expected %d metrics but got %d", len(expected.Result),
			len(actual.Result))
	}

	metricStringToIndexMap := make(map[string]int, len(expected.Result))
	for i, actualMetric := range actual.Result {
		metricStringToIndexMap[labels.FromMap(actualMetric.Metric).String()] = i
	}

	for _, expectedMetric := range expected.Result {
		actualMetricIndex, ok := metricStringToIndexMap[labels.FromMap(expectedMetric.Metric).String()]
		if !ok {
			return fmt.Errorf("expected metric %s missing from actual response", expectedMetric.Metric)
		}

		actualMetric := actual.Result[actualMetricIndex]
		expectedMetricLen := len(expectedMetric.Values)
		actualMetricLen := len(actualMetric.Values)

		if expectedMetricLen != actualMetricLen {
			err := fmt.Errorf("expected %d samples for metric %s but got %d", expectedMetricLen,
				expectedMetric.Metric, actualMetricLen)
			if expectedMetricLen > 0 && actualMetricLen > 0 {
				level.Error(util.Logger).Log("msg", err.Error(), "oldest-expected-ts", int64(expectedMetric.Values[0][0].(float64)),
					"newest-expected-ts", int64(expectedMetric.Values[expectedMetricLen-1][0].(float64)),
					"oldest-actual-ts", int64(actualMetric.Values[0][0].(float64)), "newest-actual-ts", int64(actualMetric.Values[actualMetricLen-1][0].(float64)))
			}
			return err
		}

		for i, expectedSamplePair := range expectedMetric.Values {
			actualSamplePair := actualMetric.Values[i]
			err := compareSamplePair(expectedSamplePair, actualSamplePair)
			if err != nil {
				return errors.Wrapf(err, "sample pair not matching for metric %s", expectedMetric.Metric)
			}
		}
	}

	return nil
}

type vectorResult struct {
	Result []struct {
		Metric map[string]string
		Value  []interface{}
	}
}

func compareVector(expectedRaw, actualRaw json.RawMessage) error {
	var expected, actual vectorResult

	err := json.Unmarshal(expectedRaw, &expected.Result)
	if err != nil {
		return err
	}

	err = json.Unmarshal(actualRaw, &actual.Result)
	if err != nil {
		return err
	}

	if len(expected.Result) != len(actual.Result) {
		return fmt.Errorf("expected %d metrics but got %d", len(expected.Result),
			len(actual.Result))
	}

	metricStringToIndexMap := make(map[string]int, len(expected.Result))
	for i, actualMetric := range actual.Result {
		metricStringToIndexMap[labels.FromMap(actualMetric.Metric).String()] = i
	}

	for _, expectedMetric := range expected.Result {
		actualMetricIndex, ok := metricStringToIndexMap[labels.FromMap(expectedMetric.Metric).String()]
		if !ok {
			return fmt.Errorf("expected metric %s missing from actual response", expectedMetric.Metric)
		}

		actualMetric := actual.Result[actualMetricIndex]
		err := compareSamplePair(expectedMetric.Value, actualMetric.Value)
		if err != nil {
			return errors.Wrapf(err, "sample pair not matching for metric %s", expectedMetric.Metric)
		}
	}

	return nil
}

func compareScalar(expectedRaw, actualRaw json.RawMessage) error {
	var expected, actual []interface{}
	err := json.Unmarshal(expectedRaw, &expected)
	if err != nil {
		return err
	}

	err = json.Unmarshal(actualRaw, &actual)
	if err != nil {
		return err
	}

	return compareSamplePair(expected, actual)
}

func compareSamplePair(expected, actual []interface{}) error {
	if len(expected) != 2 {
		return fmt.Errorf("expected 2 values in samplePair for expected response but got %d", len(expected))
	}

	if len(actual) != 2 {
		return fmt.Errorf("expected 2 values in samplePair but got %d", len(actual))
	}

	if expected[0] != actual[0] {
		return fmt.Errorf("expected timestamp %d but got %d", int64(expected[0].(float64)), int64(actual[0].(float64)))
	}
	if expected[1] != actual[1] {
		return fmt.Errorf("expected value %s for timestamp %d but got %s", expected[1].(string), int64(expected[0].(float64)), actual[1].(string))
	}

	return nil
}
