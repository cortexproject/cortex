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

func (t timed) GetOrgConfig(orgID configs.OrgID) (cfg configs.ConfigView, err error) {
	t.timeRequest("GetOrgConfig", func(_ context.Context) error {
		cfg, err = t.d.GetOrgConfig(orgID)
		return err
	})
	return
}

func (t timed) SetOrgConfig(orgID configs.OrgID, cfg configs.Config) (err error) {
	return t.timeRequest("SetOrgConfig", func(_ context.Context) error {
		return t.d.SetOrgConfig(orgID, cfg)
	})
}

func (t timed) GetAllOrgConfigs() (cfgs map[configs.OrgID]configs.ConfigView, err error) {
	t.timeRequest("GetAllOrgConfigs", func(_ context.Context) error {
		cfgs, err = t.d.GetAllOrgConfigs()
		return err
	})
	return
}

func (t timed) GetOrgConfigs(since configs.ID) (cfgs map[configs.OrgID]configs.ConfigView, err error) {
	t.timeRequest("GetOrgConfigs", func(_ context.Context) error {
		cfgs, err = t.d.GetOrgConfigs(since)
		return err
	})
	return
}

func (t timed) Close() error {
	return t.timeRequest("Close", func(_ context.Context) error {
		return t.d.Close()
	})
}
