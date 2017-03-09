package db

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/cortex/configs"
	"golang.org/x/net/context"
)

var (
	databaseRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "database_request_duration_seconds",
		Help:      "Time spent (in seconds) doing database requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "status_code"})
)

func init() {
	prometheus.MustRegister(databaseRequestDuration)
}

// timed adds prometheus timings to another database implementation
type timed struct {
	d DB
}

func (t timed) errorCode(err error) string {
	switch err {
	case nil:
		return "200"
	default:
		return "500"
	}
}

func (t timed) timeRequest(method string, f func(context.Context) error) error {
	return instrument.TimeRequestHistogramStatus(context.TODO(), method, databaseRequestDuration, t.errorCode, f)
}

func (t timed) GetConfig(userID string) (cfg configs.ConfigView, err error) {
	t.timeRequest("GetConfig", func(_ context.Context) error {
		cfg, err = t.d.GetConfig(userID)
		return err
	})
	return
}

func (t timed) SetConfig(userID string, cfg configs.Config) (err error) {
	return t.timeRequest("SetConfig", func(_ context.Context) error {
		return t.d.SetConfig(userID, cfg)
	})
}

func (t timed) GetAllConfigs() (cfgs map[string]configs.ConfigView, err error) {
	t.timeRequest("GetAllConfigs", func(_ context.Context) error {
		cfgs, err = t.d.GetAllConfigs()
		return err
	})
	return
}

func (t timed) GetConfigs(since configs.ID) (cfgs map[string]configs.ConfigView, err error) {
	t.timeRequest("GetConfigs", func(_ context.Context) error {
		cfgs, err = t.d.GetConfigs(since)
		return err
	})
	return
}

func (t timed) Close() error {
	return t.timeRequest("Close", func(_ context.Context) error {
		return t.d.Close()
	})
}
