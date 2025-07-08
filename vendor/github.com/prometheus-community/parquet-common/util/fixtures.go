package util

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
)

type TestConfig struct {
	TotalMetricNames     int
	MetricsPerMetricName int
	NumberOfLabels       int
	RandomLabels         int
	NumberOfSamples      int
}

func DefaultTestConfig() TestConfig {
	return TestConfig{
		TotalMetricNames:     1_000,
		MetricsPerMetricName: 20,
		NumberOfLabels:       5,
		RandomLabels:         3,
		NumberOfSamples:      250,
	}
}

type TestData struct {
	SeriesHash map[uint64]*struct{}
	MinTime    int64
	MaxTime    int64
}

func GenerateTestData(t *testing.T, st *teststorage.TestStorage, ctx context.Context, cfg TestConfig) TestData {
	app := st.Appender(ctx)
	seriesHash := make(map[uint64]*struct{})
	builder := labels.NewScratchBuilder(cfg.NumberOfLabels)

	for i := 0; i < cfg.TotalMetricNames; i++ {
		for n := 0; n < cfg.MetricsPerMetricName; n++ {
			builder.Reset()
			builder.Add(labels.MetricName, fmt.Sprintf("metric_%d", i))
			builder.Add("unique", fmt.Sprintf("unique_%d", n))

			for j := 0; j < cfg.NumberOfLabels; j++ {
				builder.Add(fmt.Sprintf("label_name_%v", j), fmt.Sprintf("label_value_%v", j))
			}

			firstRandom := rand.Int() % 10
			for k := firstRandom; k < firstRandom+cfg.RandomLabels; k++ {
				builder.Add(fmt.Sprintf("random_name_%v", k), fmt.Sprintf("random_value_%v", k))
			}

			builder.Sort()
			lbls := builder.Labels()
			seriesHash[lbls.Hash()] = &struct{}{}
			for s := 0; s < cfg.NumberOfSamples; s++ {
				_, err := app.Append(0, lbls, (1 * time.Minute * time.Duration(s)).Milliseconds(), float64(i))
				require.NoError(t, err)
			}
		}
	}

	require.NoError(t, app.Commit())
	h := st.Head()

	return TestData{
		SeriesHash: seriesHash,
		MinTime:    h.MinTime(),
		MaxTime:    h.MaxTime(),
	}
}
