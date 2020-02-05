package extprom

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type TxGaugeVec struct {
	current      *prometheus.GaugeVec
	mtx          sync.Mutex
	newMetricVal func() *prometheus.GaugeVec

	tx *prometheus.GaugeVec
}

// NewTxGaugeVec is a prometheus.GaugeVec that allows to start atomic metric value transaction.
// It might be useful if long process that wants to update a GaugeVec but wants to build/accumulate those metrics
// in a concurrent way without exposing partial state to Prometheus.
// Caller can also use this as normal GaugeVec.
//
// Additionally it allows to init LabelValues on each transaction.
// NOTE: This is quite naive implementation creating new prometheus.GaugeVec on each `ResetTx`, use wisely.
func NewTxGaugeVec(opts prometheus.GaugeOpts, labelNames []string, initLabelValues ...[]string) *TxGaugeVec {
	f := func() *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(opts, labelNames)
		for _, vals := range initLabelValues {
			g.WithLabelValues(vals...)
		}
		return g
	}
	return &TxGaugeVec{
		current:      f(),
		newMetricVal: f,
	}
}

// ResetTx starts new transaction. Not goroutine-safe.
func (tx *TxGaugeVec) ResetTx() {
	tx.tx = tx.newMetricVal()
}

// Submit atomically and fully applies new values from existing transaction GaugeVec. Not goroutine-safe.
func (tx *TxGaugeVec) Submit() {
	if tx.tx == nil {
		return
	}

	tx.mtx.Lock()
	tx.current = tx.tx
	tx.mtx.Unlock()
}

// Describe is used in Register.
func (tx *TxGaugeVec) Describe(ch chan<- *prometheus.Desc) {
	tx.mtx.Lock()
	defer tx.mtx.Unlock()

	tx.current.Describe(ch)
}

// Collect is used by Registered.
func (tx *TxGaugeVec) Collect(ch chan<- prometheus.Metric) {
	tx.mtx.Lock()
	defer tx.mtx.Unlock()

	tx.current.Collect(ch)
}

// With works as GetMetricWith, but panics where GetMetricWithLabels would have
// returned an error. Not returning an error allows shortcuts like
//     myVec.With(prometheus.Labels{"code": "404", "method": "GET"}).Add(42)
func (tx *TxGaugeVec) With(labels prometheus.Labels) prometheus.Gauge {
	if tx.tx == nil {
		tx.ResetTx()
	}
	return tx.tx.With(labels)
}

// WithLabelValues works as GetMetricWithLabelValues, but panics where
// GetMetricWithLabelValues would have returned an error. Not returning an
// error allows shortcuts like
//     myVec.WithLabelValues("404", "GET").Add(42)
func (tx *TxGaugeVec) WithLabelValues(lvs ...string) prometheus.Gauge {
	if tx.tx == nil {
		tx.ResetTx()
	}
	return tx.tx.WithLabelValues(lvs...)
}
