package e2e

import (
	"context"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func RunCommandAndGetOutput(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	return cmd.CombinedOutput()
}

func RunCommandWithTimeoutAndGetOutput(timeout time.Duration, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.CombinedOutput()
}

func EmptyFlags() map[string]string {
	return map[string]string{}
}

func MergeFlags(inputs ...map[string]string) map[string]string {
	output := MergeFlagsWithoutRemovingEmpty(inputs...)

	for k, v := range output {
		if v == "" {
			delete(output, k)
		}
	}

	return output
}

func MergeFlagsWithoutRemovingEmpty(inputs ...map[string]string) map[string]string {
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

func PostRequest(url string) (*http.Response, error) {
	const timeout = 1 * time.Second

	client := &http.Client{Timeout: timeout}
	return client.Post(url, "", strings.NewReader(""))
}

// TimeToMilliseconds returns the input time as milliseconds, using the same
// formula used by Prometheus in order to get the same timestamp when asserting
// on query results. The formula we're mimicking here is Prometheus parseTime().
// See: https://github.com/prometheus/prometheus/blob/df80dc4d3970121f2f76cba79050983ffb3cdbb0/web/api/v1/api.go#L1690-L1694
func TimeToMilliseconds(t time.Time) int64 {
	// Convert to seconds.
	sec := float64(t.Unix()) + float64(t.Nanosecond())/1e9

	// Parse seconds.
	s, ns := math.Modf(sec)

	// Round nanoseconds part.
	ns = math.Round(ns*1000) / 1000

	// Convert to millis.
	return (int64(s) * 1e3) + (int64(ns * 1e3))
}

func GenerateSeries(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector) {
	tsMillis := TimeToMilliseconds(ts)
	value := rand.Float64()

	lbls := append(
		[]prompb.Label{
			{Name: labels.MetricName, Value: name},
		},
		additionalLabels...,
	)

	// Generate the series
	series = append(series, prompb.TimeSeries{
		Labels: lbls,
		Samples: []prompb.Sample{
			{Value: value, Timestamp: tsMillis},
		},
	})

	// Generate the expected vector when querying it
	metric := model.Metric{}
	metric[labels.MetricName] = model.LabelValue(name)
	for _, lbl := range additionalLabels {
		metric[model.LabelName(lbl.Name)] = model.LabelValue(lbl.Value)
	}

	vector = append(vector, &model.Sample{
		Metric:    metric,
		Value:     model.SampleValue(value),
		Timestamp: model.Time(tsMillis),
	})

	return
}

func GenerateSeriesWithSamples(
	name string,
	startTime time.Time,
	scrapeInterval time.Duration,
	startValue int,
	numSamples int,
	additionalLabels ...prompb.Label,
) (series prompb.TimeSeries) {
	tsMillis := TimeToMilliseconds(startTime)
	durMillis := scrapeInterval.Milliseconds()

	lbls := append(
		[]prompb.Label{
			{Name: labels.MetricName, Value: name},
		},
		additionalLabels...,
	)

	startTMillis := tsMillis
	samples := make([]prompb.Sample, numSamples)
	for i := 0; i < numSamples; i++ {
		samples[i] = prompb.Sample{
			Timestamp: startTMillis,
			Value:     float64(i + startValue),
		}
		startTMillis += durMillis
	}

	return prompb.TimeSeries{
		Labels:  lbls,
		Samples: samples,
	}
}

// GetTempDirectory creates a temporary directory for shared integration
// test files, either in the working directory or a directory referenced by
// the E2E_TEMP_DIR environment variable
func GetTempDirectory() (string, error) {
	var (
		dir string
		err error
	)
	// If a temp dir is referenced, return that
	if os.Getenv("E2E_TEMP_DIR") != "" {
		dir = os.Getenv("E2E_TEMP_DIR")
	} else {
		dir, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}

	tmpDir, err := os.MkdirTemp(dir, "e2e_integration_test")
	if err != nil {
		return "", err
	}
	absDir, err := filepath.Abs(tmpDir)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", err
	}

	return absDir, nil
}

func RandRange(rnd *rand.Rand, min, max int64) int64 {
	return rnd.Int63n(max-min) + min
}

func CreateBlock(
	ctx context.Context,
	rnd *rand.Rand,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	scrapeInterval int64,
	seriesSize int64,
) (id ulid.ULID, err error) {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = 10000000000
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	if err != nil {
		return id, errors.Wrap(err, "create head block")
	}
	defer func() {
		runutil.CloseWithErrCapture(&err, h, "TSDB Head")
		if e := os.RemoveAll(headOpts.ChunkDirRoot); e != nil {
			err = errors.Wrap(e, "delete chunks dir")
		}
	}()

	app := h.Appender(ctx)
	for i := 0; i < len(series); i++ {

		var ref storage.SeriesRef
		start := RandRange(rnd, mint, maxt)
		for j := 0; j < numSamples; j++ {
			ref, err = app.Append(ref, series[i], start, float64(i+j))
			if err != nil {
				if rerr := app.Rollback(); rerr != nil {
					err = errors.Wrapf(err, "rollback failed: %v", rerr)
				}
				return id, errors.Wrap(err, "add sample")
			}
			start += scrapeInterval
			if start > maxt {
				break
			}
		}
	}
	if err := app.Commit(); err != nil {
		return id, errors.Wrap(err, "commit")
	}

	c, err := tsdb.NewLeveledCompactor(ctx, nil, log.NewNopLogger(), []int64{maxt - mint}, nil, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}

	ids, err := c.Write(dir, h, mint, maxt, nil)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}
	if len(ids) == 0 {
		return id, errors.Errorf("nothing to write, asked for %d samples", numSamples)
	}
	id = ids[0]

	blockDir := filepath.Join(dir, id.String())
	logger := log.NewNopLogger()

	if _, err = metadata.InjectThanos(logger, blockDir, metadata.Thanos{
		Labels: map[string]string{
			cortex_tsdb.IngesterIDExternalLabel: "ingester-0",
		},
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
		IndexStats: metadata.IndexStats{SeriesMaxSize: seriesSize},
	}, nil); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	return id, nil
}
