package ruler

import (
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	blockedWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "blocked_workers",
		Help:      "How many workers are waiting on an item to be ready.",
	})
	workerIdleTime = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "worker_idle_seconds_total",
		Help:      "How long workers have spent waiting for work.",
	})
	evalLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "group_evaluation_latency_seconds",
		Help:      "How far behind the target time each rule group executed.",
		Buckets:   []float64{.1, .25, .5, 1, 2.5, 5, 10, 25},
	})
)

// Worker does a thing until it's told to stop.
type Worker interface {
	Run()
	Stop()
}

type worker struct {
	scheduler *scheduler
	ruler     *Ruler
}

func newWorker(ruler *Ruler) worker {
	return worker{
		scheduler: ruler.scheduler,
		ruler:     ruler,
	}
}

func (w *worker) Run() {
	for {
		waitStart := time.Now()
		blockedWorkers.Inc()
		level.Debug(util.Logger).Log("msg", "waiting for next work item")
		item := w.scheduler.nextWorkItem()
		blockedWorkers.Dec()
		waitElapsed := time.Now().Sub(waitStart)
		if item == nil {
			level.Debug(util.Logger).Log("msg", "queue closed and empty; terminating worker")
			return
		}
		latency := time.Since(item.scheduled)
		evalLatency.Observe(latency.Seconds())
		workerIdleTime.Add(waitElapsed.Seconds())
		level.Debug(util.Logger).Log("msg", "processing item", "item", item, "latency", latency.String())
		w.ruler.Evaluate(item.userID, item)
		w.scheduler.workItemDone(*item)
		level.Debug(util.Logger).Log("msg", "item handed back to queue", "item", item)
	}
}
