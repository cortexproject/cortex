package querier

import (
	"context"

	prom_stats "github.com/prometheus/prometheus/util/stats"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/cortexproject/cortex/pkg/querier/stats"
)

func StatsRenderer(ctx context.Context, promStat *prom_stats.Statistics, param string) prom_stats.QueryStats {
	queryStat := stats.FromContext(ctx)
	if queryStat != nil && promStat != nil {
		queryStat.AddScannedSamples(uint64(promStat.Samples.TotalSamples))
		queryStat.AddPeakSamples(uint64(promStat.Samples.PeakSamples))
	}

	return v1.DefaultStatsRenderer(ctx, promStat, param)
}
