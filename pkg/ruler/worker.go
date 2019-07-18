package ruler

import (
	"context"
	native_ctx "context"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/user"
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
		// Grab next scheduled item from the queue
		level.Debug(util.Logger).Log("msg", "waiting for next work item")
		waitStart := time.Now()

		blockedWorkers.Inc()
		item := w.scheduler.nextWorkItem()
		blockedWorkers.Dec()

		waitElapsed := time.Now().Sub(waitStart)
		workerIdleTime.Add(waitElapsed.Seconds())

		// If no item is returned, worker is safe to terminate
		if item == nil {
			level.Debug(util.Logger).Log("msg", "queue closed and empty; terminating worker")
			return
		}

		w.Evaluate(item.userID, item)
		w.scheduler.workItemDone(*item)
		level.Debug(util.Logger).Log("msg", "item handed back to queue", "item", item)
	}
}

// Evaluate a list of rules in the given context.
func (w *worker) Evaluate(userID string, item *workItem) {
	ctx := user.InjectOrgID(context.Background(), userID)
	logger := util.WithContext(ctx, util.Logger)
	if w.ruler.cfg.EnableSharding && !w.ruler.ownsRule(item.hash) {
		level.Debug(util.Logger).Log("msg", "ruler: skipping evaluation, not owned", "user_id", item.userID, "group", item.groupID)
		return
	}
	level.Debug(logger).Log("msg", "evaluating rules...", "num_rules", len(item.group.Rules()))

	instrument.CollectedRequest(ctx, "Evaluate", evalDuration, nil, func(ctx native_ctx.Context) error {
		if span := opentracing.SpanFromContext(ctx); span != nil {
			span.SetTag("instance", userID)
			span.SetTag("groupID", item.groupID)
		}
		item.group.Eval(ctx, time.Now())
		return nil
	})
}
