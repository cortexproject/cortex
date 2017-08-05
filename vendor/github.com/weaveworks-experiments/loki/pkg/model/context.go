package model

// ForeachBaggageItem implements opentracing.SpanContext
func (s SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	for _, b := range s.Baggage {
		if !handler(b.Key, b.Value) {
			return
		}
	}
}

func (s SpanContext) baggageItem(k string) string {
	for _, b := range s.Baggage {
		if b.Key == k {
			return b.Value
		}
	}
	return ""
}

func (s SpanContext) withBaggageItem(k, v string) SpanContext {
	result := s
	result.Baggage = append(result.Baggage, Baggage{
		Key:   k,
		Value: v,
	})
	return result
}
