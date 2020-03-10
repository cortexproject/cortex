package e2e

import (
	"math"
	"math/rand"
	"net/http"
	"os/exec"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

func RunCommandAndGetOutput(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	return cmd.CombinedOutput()
}

func EmptyFlags() map[string]string {
	return map[string]string{}
}

func MergeFlags(inputs ...map[string]string) map[string]string {
	output := map[string]string{}

	for _, input := range inputs {
		for name, value := range input {
			output[name] = value
		}
	}

	for k, v := range output {
		if v == "" {
			delete(output, k)
		}
	}

	return output
}

func BuildArgs(flags map[string]string) []string {
	args := make([]string, 0, len(flags))

	for name, value := range flags {
		if value != "" {
			args = append(args, name+"="+value)
		} else {
			args = append(args, name)
		}
	}

	return args
}

func GetRequest(url string) (*http.Response, error) {
	const timeout = 1 * time.Second

	client := &http.Client{Timeout: timeout}
	return client.Get(url)
}

// timeToMilliseconds returns the input time as milliseconds, using the same
// formula used by Prometheus in order to get the same timestamp when asserting
// on query results.
func TimeToMilliseconds(t time.Time) int64 {
	// The millisecond is rounded to the nearest
	return int64(math.Round(float64(t.UnixNano()) / 1000000))
}

func GenerateSeries(name string, ts time.Time) (series []prompb.TimeSeries, vector model.Vector) {
	tsMillis := TimeToMilliseconds(ts)
	value := rand.Float64()

	// Generate the series
	series = append(series, prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: labels.MetricName, Value: name},
		},
		Samples: []prompb.Sample{
			{Value: value, Timestamp: tsMillis},
		},
	})

	// Generate the expected vector when querying it
	metric := model.Metric{}
	metric[labels.MetricName] = model.LabelValue(name)

	vector = append(vector, &model.Sample{
		Metric:    metric,
		Value:     model.SampleValue(value),
		Timestamp: model.Time(tsMillis),
	})

	return
}
