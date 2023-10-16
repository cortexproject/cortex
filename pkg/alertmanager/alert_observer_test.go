package alertmanager

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/alertmanager/alertobserver"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"
	"github.com/stretchr/testify/assert"
)

func TestAlertLifeCycleObserverLimiter(t *testing.T) {
	tenant := "fake"
	lim := limits{
		tenant: tenant,
		limit:  2,
	}
	limiter := NewAlertLifeCycleObserverLimiter(tenant, lim)
	assert.Equal(t, 2, limiter.Level())
}

type limits struct {
	tenant string
	limit  int
}

func (l limits) AlertmanagerAlertLifeCycleObserverLevel(tenant string) int {
	if tenant == l.tenant {
		return l.limit
	}
	return 0
}

func TestLogAlertLifeCycleObserver(t *testing.T) {
	logger := &FakeLogger{}
	alerts := []*types.Alert{{}, {}}
	ctx := context.Background()
	ctx = notify.WithReceiverName(ctx, "rcv")
	ctx = notify.WithGroupKey(ctx, "key")

	for _, tc := range []struct {
		event               string
		logLvl              []int
		alerts              []*types.Alert
		meta                alertobserver.AlertEventMeta
		expectedMsg         string
		expectedLogCount    int
		expectedLoggedKeys  []string
		expectedMissingKeys []string
	}{
		{
			event:            alertobserver.EventAlertAddedToAggrGroup,
			alerts:           alerts,
			meta:             alertobserver.AlertEventMeta{},
			logLvl:           []int{0},
			expectedLogCount: 0,
		},
		{
			event:            alertobserver.EventAlertReceived,
			alerts:           alerts,
			meta:             alertobserver.AlertEventMeta{},
			logLvl:           []int{1, 2, 3, 4},
			expectedLogCount: 0,
		},
		{
			event:              alertobserver.EventAlertReceived,
			alerts:             alerts,
			meta:               alertobserver.AlertEventMeta{},
			logLvl:             []int{5},
			expectedLogCount:   2,
			expectedMsg:        "Received",
			expectedLoggedKeys: []string{"labels", "fingerprint"},
		},
		{
			event:              alertobserver.EventAlertRejected,
			alerts:             alerts,
			meta:               alertobserver.AlertEventMeta{"msg": "test"},
			logLvl:             []int{1, 2, 3, 4, 5},
			expectedLogCount:   2,
			expectedMsg:        "Rejected",
			expectedLoggedKeys: []string{"reason", "labels", "fingerprint"},
		},
		{
			event:              alertobserver.EventAlertAddedToAggrGroup,
			alerts:             alerts,
			meta:               alertobserver.AlertEventMeta{"groupKey": "test"},
			logLvl:             []int{1, 2, 3, 4, 5},
			expectedLogCount:   2,
			expectedMsg:        "Added to aggregation group",
			expectedLoggedKeys: []string{"groupKey", "labels", "fingerprint"},
		},
		{
			event:              alertobserver.EventAlertFailedAddToAggrGroup,
			alerts:             alerts,
			meta:               alertobserver.AlertEventMeta{"groupKey": "test"},
			logLvl:             []int{1, 2, 3, 4, 5},
			expectedLogCount:   2,
			expectedMsg:        "Failed to add aggregation group",
			expectedLoggedKeys: []string{"reason", "labels", "fingerprint"},
		},
		{
			event:            alertobserver.EventAlertPipelineStart,
			alerts:           alerts,
			meta:             alertobserver.AlertEventMeta{"ctx": ctx},
			logLvl:           []int{1},
			expectedLogCount: 0,
		},
		{
			event:               alertobserver.EventAlertPipelineStart,
			alerts:              alerts,
			meta:                alertobserver.AlertEventMeta{"ctx": ctx},
			logLvl:              []int{2, 3, 4},
			expectedLogCount:    1,
			expectedMsg:         "Entered the pipeline",
			expectedLoggedKeys:  []string{"groupKey", "receiver"},
			expectedMissingKeys: []string{"fingerprint"},
		},
		{
			event:               alertobserver.EventAlertPipelineStart,
			alerts:              alerts,
			meta:                alertobserver.AlertEventMeta{"ctx": ctx},
			logLvl:              []int{5},
			expectedLogCount:    2,
			expectedMsg:         "Entered the pipeline",
			expectedLoggedKeys:  []string{"groupKey", "receiver", "fingerprint"},
			expectedMissingKeys: []string{"labels"},
		},
		{
			event:            alertobserver.EventAlertPipelinePassStage,
			alerts:           alerts,
			meta:             alertobserver.AlertEventMeta{"ctx": ctx, "stageName": "FanoutStage"},
			logLvl:           []int{1000},
			expectedLogCount: 0,
		},
		{
			event:            alertobserver.EventAlertPipelinePassStage,
			alerts:           alerts,
			meta:             alertobserver.AlertEventMeta{"ctx": ctx, "stageName": "Notify"},
			logLvl:           []int{1, 2},
			expectedLogCount: 0,
		},
		{
			event:               alertobserver.EventAlertPipelinePassStage,
			alerts:              alerts,
			meta:                alertobserver.AlertEventMeta{"ctx": ctx, "stageName": "Notify"},
			logLvl:              []int{3, 4, 5},
			expectedLogCount:    1,
			expectedMsg:         "Passed stage",
			expectedLoggedKeys:  []string{"groupKey", "receiver", "stage"},
			expectedMissingKeys: []string{"fingerprint"},
		},
		{
			event:               alertobserver.EventAlertSent,
			alerts:              alerts,
			meta:                alertobserver.AlertEventMeta{"ctx": ctx, "integration": "sns"},
			logLvl:              []int{1, 2, 3},
			expectedLogCount:    1,
			expectedMsg:         "Sent",
			expectedLoggedKeys:  []string{"groupKey", "receiver"},
			expectedMissingKeys: []string{"fingerprint"},
		},
		{
			event:               alertobserver.EventAlertSent,
			alerts:              alerts,
			meta:                alertobserver.AlertEventMeta{"ctx": ctx, "integration": "sns"},
			logLvl:              []int{4, 5},
			expectedLogCount:    2,
			expectedMsg:         "Sent",
			expectedLoggedKeys:  []string{"groupKey", "receiver", "fingerprint"},
			expectedMissingKeys: []string{"labels"},
		},
		{
			event:               alertobserver.EventAlertSendFailed,
			alerts:              alerts,
			meta:                alertobserver.AlertEventMeta{"ctx": ctx, "integration": "sns"},
			logLvl:              []int{1, 2, 3},
			expectedLogCount:    1,
			expectedMsg:         "Send failed",
			expectedLoggedKeys:  []string{"groupKey", "receiver"},
			expectedMissingKeys: []string{"fingerprint"},
		},
		{
			event:               alertobserver.EventAlertSendFailed,
			alerts:              alerts,
			meta:                alertobserver.AlertEventMeta{"ctx": ctx, "integration": "sns"},
			logLvl:              []int{4, 5},
			expectedLogCount:    2,
			expectedMsg:         "Send failed",
			expectedLoggedKeys:  []string{"groupKey", "receiver", "fingerprint"},
			expectedMissingKeys: []string{"labels"},
		},
		{
			event:               alertobserver.EventAlertMuted,
			alerts:              alerts,
			meta:                alertobserver.AlertEventMeta{"ctx": ctx},
			logLvl:              []int{1, 2, 3, 4, 5},
			expectedLogCount:    2,
			expectedMsg:         "Muted",
			expectedLoggedKeys:  []string{"groupKey", "fingerprint"},
			expectedMissingKeys: []string{"labels"},
		},
	} {
		tc := tc
		for _, logLvl := range tc.logLvl {
			logger.clear()
			l := NewAlertLifeCycleObserverLimiter("fake", limits{tenant: "fake", limit: logLvl})
			o := NewLogAlertLifeCycleObserver(logger, "fake", l)
			o.Observe(tc.event, tc.alerts, tc.meta)
			assert.Equal(t, tc.expectedLogCount, len(logger.loggedValues))
			for i := 0; i < tc.expectedLogCount; i++ {
				loggedValues := logger.loggedValues[i]
				assert.Equal(t, tc.expectedMsg, loggedValues["msg"])
				for _, v := range tc.expectedLoggedKeys {
					_, ok := loggedValues[v]
					assert.True(t, ok, fmt.Sprintf("'%v' is missing from the log", v))
				}
				for _, v := range tc.expectedMissingKeys {
					_, ok := loggedValues[v]
					assert.False(t, ok, fmt.Sprintf("'%v' should be excluded from the log", v))
				}
			}
		}
	}
}

type FakeLogger struct {
	loggedValues []map[string]string
	mtx          sync.RWMutex
}

func (l *FakeLogger) Log(keyvals ...interface{}) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	params := make(map[string]string)
	for i, v := range keyvals {
		if i%2 == 0 {
			params[fmt.Sprintf("%v", v)] = fmt.Sprintf("%v", keyvals[i+1])
		}
	}
	l.loggedValues = append(l.loggedValues, params)
	return nil
}

func (l *FakeLogger) clear() {
	l.loggedValues = l.loggedValues[:0]
}
