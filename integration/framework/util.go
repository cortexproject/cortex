package framework

import (
	"math"
	"net/http"
	"net/url"
	"os/exec"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	awscommon "github.com/weaveworks/common/aws"
)

func RunCommandAndGetOutput(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	return cmd.CombinedOutput()
}

func MergeFlags(inputs ...map[string]string) map[string]string {
	output := map[string]string{}

	for _, input := range inputs {
		for name, value := range input {
			output[name] = value
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

func NewDynamoClient(endpoint string) (*dynamodb.DynamoDB, error) {
	dynamoURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	dynamoConfig, err := awscommon.ConfigFromURL(dynamoURL)
	if err != nil {
		return nil, err
	}

	dynamoConfig = dynamoConfig.WithMaxRetries(0)
	dynamoSession, err := session.NewSession(dynamoConfig)
	if err != nil {
		return nil, err
	}

	return dynamodb.New(dynamoSession), nil
}

func GenerateSeries(name string, ts time.Time) (series []prompb.TimeSeries, vector model.Vector) {
	tsMillis := TimeToMilliseconds(ts)

	// Generate the series
	series = append(series, prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: labels.MetricName, Value: name},
		},
		Samples: []prompb.Sample{
			{Value: float64(1), Timestamp: tsMillis},
		},
	})

	// Generate the expected vector when querying it
	metric := model.Metric{}
	metric[labels.MetricName] = model.LabelValue(name)

	vector = append(vector, &model.Sample{
		Metric:    metric,
		Value:     model.SampleValue(1),
		Timestamp: model.Time(tsMillis),
	})

	return
}
