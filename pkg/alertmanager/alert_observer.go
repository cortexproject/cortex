package alertmanager

import (
	"context"
	"strings"

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

func (a *AlertLifeCycleObserverLimiter) Level() int {
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
	if alerts == nil || o.limiter == nil || o.limiter.Level() <= 0 || !o.shouldLog(event) {
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
		o.PipelinePassStage(alerts, meta)
	case alertobserver.EventAlertSent:
		o.Sent(alerts, meta)
	case alertobserver.EventAlertSendFailed:
		o.SendFailed(alerts, meta)
	case alertobserver.EventAlertMuted:
		o.Muted(alerts, meta)
	}
}

func (o *LogAlertLifeCycleObserver) shouldLog(event string) bool {
	// The general idea of having levels in the limiter is to control the volume of logs AM is producing. The observer
	// logs many types of events and some events are more important than others. By configuring the level we can
	// continue to have observability on the alerts at lower granularity if we see that the volume of logs is getting
	// too expensive.
	// What is log per level is as follows:
	//* 1
	//	* Alert is rejected because of validation error
	//	* Alert joins an aggregation group
	//	* Alert is muted
	//	* Aggregation Group notification Sent / Failed
	//* 2
	//	* Alert is rejected because of validation error
	//	* Alert joins an aggregation group
	//	* Alert is muted
	//	* Aggregation Group pipeline start
	//	* Aggregation Group notification Sent / Failed
	//* 3
	//	* Alert is rejected because of validation error
	//	* Alert joins an aggregation group
	//	* Alert is muted
	//	* Aggregation Group pipeline start
	//	* Aggregation Group passed a stage in the pipeline
	//	* Aggregation Group notification Sent / Failed
	//* 4
	//	* Alert is rejected because of validation error
	//	* Alert joins an aggregation group
	//	* Alert is muted
	//	* Aggregation group pipeline start
	//	* Aggregation Group passed a stage in the pipeline
	//	* Alert in aggregation group is Sent / Failed
	//* 5
	//	* Alert is rejected because of validation error
	//	* Alert is received
	//	* Alert joins an aggregation group
	//	* Alert is muted
	//	* Alert in aggregation group pipeline start
	//	* Aggregation Group passed a stage in the pipeline
	//	* Alert in aggregation group is Sent / Failed
	if o.limiter == nil || o.limiter.Level() <= 0 {
		return false
	}
	logLvl := o.limiter.Level()
	switch event {
	case alertobserver.EventAlertReceived:
		if logLvl < 5 {
			return false
		}
	case alertobserver.EventAlertPipelineStart:
		if logLvl < 2 {
			return false
		}
	case alertobserver.EventAlertPipelinePassStage:
		if logLvl < 3 {
			return false
		}
	}
	return true
}

func (o *LogAlertLifeCycleObserver) Received(alerts []*types.Alert) {
	for _, a := range alerts {
		o.logWithAlertWithLabels(a, "msg", "Received")
	}
}

func (o *LogAlertLifeCycleObserver) Rejected(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	reason, ok := meta["msg"]
	if !ok {
		reason = "Unknown"
	}
	for _, a := range alerts {
		o.logWithAlertWithLabels(a, "msg", "Rejected", "reason", reason)
	}
}

func (o *LogAlertLifeCycleObserver) AddedAggrGroup(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	groupKey, ok := meta["groupKey"]
	if !ok {
		return
	}
	for _, a := range alerts {
		o.logWithAlertWithLabels(a, "msg", "Added to aggregation group", "groupKey", groupKey)
	}
}

func (o *LogAlertLifeCycleObserver) FailedAddToAggrGroup(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
	reason, ok := meta["msg"]
	if !ok {
		reason = "Unknown"
	}
	for _, a := range alerts {
		o.logWithAlertWithLabels(a, "msg", "Failed to add aggregation group", "reason", reason)
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
	if o.limiter.Level() < 5 {
		level.Info(o.logger).Log("msg", "Entered the pipeline", "groupKey", groupKey, "receiver", receiver, "alertsCount", len(alerts))
	} else {
		o.logWithAlertFingerprints(alerts, "msg", "Entered the pipeline", "groupKey", groupKey, "receiver", receiver)
	}
}

func (o *LogAlertLifeCycleObserver) PipelinePassStage(alerts []*types.Alert, meta alertobserver.AlertEventMeta) {
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
	level.Info(o.logger).Log("msg", "Passed stage", "groupKey", groupKey, "receiver", receiver, "stage", stageName, "alertsCount", len(alerts))
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
	if o.limiter.Level() < 4 {
		level.Info(o.logger).Log("msg", "Sent", "groupKey", groupKey, "receiver", receiver, "integration", integration, "alertsCount", len(alerts))
	} else {
		o.logWithAlertFingerprints(alerts, "msg", "Sent", "groupKey", groupKey, "receiver", receiver, "integration", integration)
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
	if o.limiter.Level() < 4 {
		level.Info(o.logger).Log("msg", "Send failed", "groupKey", groupKey, "receiver", receiver, "integration", integration, "alertsCount", len(alerts))
	} else {
		o.logWithAlertFingerprints(alerts, "msg", "Send failed", "groupKey", groupKey, "receiver", receiver, "integration", integration)
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
	o.logWithAlertFingerprints(alerts, "msg", "Muted", "groupKey", groupKey)
}

func (o *LogAlertLifeCycleObserver) logWithAlertWithLabels(alert *types.Alert, keyvals ...interface{}) {
	keyvals = append(
		keyvals,
		"fingerprint",
		alert.Fingerprint().String(),
		"start",
		alert.StartsAt.Unix(),
		"end",
		alert.EndsAt.Unix(),
		"labels",
		alert.Labels.String(),
	)
	level.Info(o.logger).Log(keyvals...)
}

func (o *LogAlertLifeCycleObserver) logWithAlertFingerprints(alerts []*types.Alert, keyvals ...interface{}) {
	fps := make([]string, len(alerts))
	for i, a := range alerts {
		fps[i] = a.Fingerprint().String()
	}
	keyvals = append(
		keyvals,
		"fingerprints",
		strings.Join(fps, ","),
	)
	level.Info(o.logger).Log(keyvals...)
}
