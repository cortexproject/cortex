package codec

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/weaveworks/common/middleware"
)

type InstrumentedCodecMetrics struct {
	responseSizeHistogram *prometheus.HistogramVec
}

func NewInstrumentedCodecMetrics(reg prometheus.Registerer) *InstrumentedCodecMetrics {
	return &InstrumentedCodecMetrics{
		responseSizeHistogram: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace:                       "cortex",
			Name:                            "querier_codec_response_size",
			Help:                            "Size of the encoded prometheus response from the queriers.",
			Buckets:                         middleware.BodySizeBuckets,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"content_type"}),
	}
}

type InstrumentedCodec struct {
	uc v1.Codec

	metrics *InstrumentedCodecMetrics
}

func (c *InstrumentedCodec) ContentType() v1.MIMEType {
	return c.uc.ContentType()
}

func (c *InstrumentedCodec) CanEncode(resp *v1.Response) bool {
	return c.uc.CanEncode(resp)
}

func (c *InstrumentedCodec) Encode(resp *v1.Response) ([]byte, error) {
	b, err := c.uc.Encode(resp)
	if err == nil {
		c.metrics.responseSizeHistogram.WithLabelValues(c.uc.ContentType().String()).Observe(float64(len((b))))
	}
	return b, err
}

func NewInstrumentedCodec(uc v1.Codec, m *InstrumentedCodecMetrics) v1.Codec {
	return &InstrumentedCodec{
		uc:      uc,
		metrics: m,
	}
}
