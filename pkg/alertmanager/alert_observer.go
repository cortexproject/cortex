package alertmanager

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/alertmanager/alertobserver"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"
)

type ObserverLimits interface {
	AlertmanagerAlertLifeCycleObserverLevel(tenant string) int
}

type AlertLifeCycleObserverLimiter struct {
	limits ObserverLimits
	tenant string
}

func NewAlertLifeCycleObserverLimiter(tenant string, limits ObserverLimits) *AlertLifeCycleObserverLimiter {
	return &AlertLifeCycleObserverLimiter{
		tenant: tenant,
		limits: limits,
	}
}

func (a *AlertLifeCycleObserverLimiter) level() int {
	return a.limits.AlertmanagerAlertLifeCycleObserverLevel(a.tenant)
}

type LogAlertLifeCycleObserver struct {
	logger  log.Logger
	limiter *AlertLifeCycleObserverLimiter
}

func NewLogAlertLifeCycleObserver(logger log.Logger, user string, limiter *AlertLifeCycleObserverLimiter) *LogAlertLifeCycleObserver {
	logger = log.With(logger, "user", user)
	logger = log.With(logger, "component", "observer")
	return &LogAlertLifeCycleObserver{
		logger:  logger,
		limiter: limiter,
	}
}

// Observe implements LifeCycleObserver
func (o *LogAlertLifeCycleObserver) Observe(event string, alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	if alerts == nil || o.limiter == nil || o.limiter.level() <= 0 {
		return
	}
	switch event {
	case alertobserver.EventAlertReceived:
		o.Received(alerts)
	case alertobserver.EventAlertRejected:
		o.Rejected(alerts, meta)
	case alertobserver.EventAlertAddedToAggrGroup:
		o.AddedAggrGroup(alerts, meta)
	case alertobserver.EventAlertFailedAddToAggrGroup:
		o.FailedAddToAggrGroup(alerts, meta)
	case alertobserver.EventAlertPipelineStart:
		o.PipelineStart(alerts, meta)
	case alertobserver.EventAlertPipelinePassStage:
		o.PipelinePassStage(meta)
	case alertobserver.EventAlertSent:
		o.Sent(alerts, meta)
	case alertobserver.EventAlertSendFailed:
		o.SendFailed(alerts, meta)
	case alertobserver.EventAlertMuted:
		o.Muted(alerts, meta)
	}
}

func (o *LogAlertLifeCycleObserver) Received(alerts []*types.Alert) {
	for _, a := range alerts {
		o.logWithAlert(a, true, "msg", "Received")
	}
}

func (o *LogAlertLifeCycleObserver) Rejected(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	reason, ok := meta["msg"]
	if !ok {
		reason = "Unknown"
	}
	for _, a := range alerts {
		o.logWithAlert(a, true, "msg", "Rejected", "reason", reason)
	}
}

func (o *LogAlertLifeCycleObserver) AddedAggrGroup(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	groupKey, ok := meta["groupKey"]
	if !ok {
		return
	}
	for _, a := range alerts {
		o.logWithAlert(a, true, "msg", "Added to aggregation group", "groupKey", groupKey)
	}
}

func (o *LogAlertLifeCycleObserver) FailedAddToAggrGroup(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	reason, ok := meta["msg"]
	if !ok {
		reason = "Unknown"
	}
	for _, a := range alerts {
		o.logWithAlert(a, true, "msg", "Failed to add aggregation group", "reason", reason)
	}
}

func (o *LogAlertLifeCycleObserver) PipelineStart(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	ctx, ok := meta["ctx"]
	if !ok {
		return
	}
	receiver, ok := notify.ReceiverName(ctx.(context.Context))
	if !ok {
		return
	}
	groupKey, ok := notify.GroupKey(ctx.(context.Context))
	if !ok {
		return
	}
	for _, a := range alerts {
		o.logWithAlert(a, false, "msg", "Entered the pipeline", "groupKey", groupKey, "receiver", receiver)
	}
}

func (o *LogAlertLifeCycleObserver) PipelinePassStage(meta alertobserver.AlertEventMeta) {
	stageName, ok := meta["stageName"]
	if !ok {
		return
	}
	if stageName == "FanoutStage" {
		// Fanout stage is just a collection of stages, so we don't really need to log it. We know if the pipeline
		// enters the Fanout stage based on the logs of its substages
		return
	}
	ctx, ok := meta["ctx"]
	if !ok {
		return
	}
	receiver, ok := notify.ReceiverName(ctx.(context.Context))
	if !ok {
		return
	}
	groupKey, ok := notify.GroupKey(ctx.(context.Context))
	if !ok {
		return
	}
	level.Info(o.logger).Log("msg", "Passed stage", "groupKey", groupKey, "receiver", receiver, "stage", stageName)
}

func (o *LogAlertLifeCycleObserver) Sent(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	ctx, ok := meta["ctx"]
	if !ok {
		return
	}
	integration, ok := meta["integration"]
	if !ok {
		return
	}
	receiver, ok := notify.ReceiverName(ctx.(context.Context))
	if !ok {
		return
	}
	groupKey, ok := notify.GroupKey(ctx.(context.Context))
	if !ok {
		return
	}
	for _, a := range alerts {
		o.logWithAlert(a, false, "msg", "Sent", "groupKey", groupKey, "receiver", receiver, "integration", integration)
	}
}

func (o *LogAlertLifeCycleObserver) SendFailed(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	ctx, ok := meta["ctx"]
	if !ok {
		return
	}
	integration, ok := meta["integration"]
	if !ok {
		return
	}
	receiver, ok := notify.ReceiverName(ctx.(context.Context))
	if !ok {
		return
	}
	groupKey, ok := notify.GroupKey(ctx.(context.Context))
	if !ok {
		return
	}
	for _, a := range alerts {
		o.logWithAlert(a, false, "msg", "Send failed", "groupKey", groupKey, "receiver", receiver, "integration", integration)
	}
}

func (o *LogAlertLifeCycleObserver) Muted(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	ctx, ok := meta["ctx"]
	if !ok {
		return
	}
	groupKey, ok := notify.GroupKey(ctx.(context.Context))
	if !ok {
		return
	}
	for _, a := range alerts {
		o.logWithAlert(a, false, "msg", "Muted", "groupKey", groupKey)
	}
}

func (o *LogAlertLifeCycleObserver) logWithAlert(alert *types.Alert, addLabels bool, keyvals ...interface{}) {
	keyvals = append(
		keyvals,
		"fingerprint",
		alert.Fingerprint().String(),
		"start",
		alert.StartsAt.Unix(),
		"end",
		alert.EndsAt.Unix(),
	)
	if addLabels {
		keyvals = append(keyvals, "labels", alert.Labels.String())
	}
	level.Info(o.logger).Log(keyvals...)
}
