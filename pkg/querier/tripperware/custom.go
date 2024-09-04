package tripperware

import (
	"fmt"

	"github.com/gogo/protobuf/types"
)

func (e *Extent) ToResponse() (Response, error) {
	msg, err := types.EmptyAny(e.Response)
	if err != nil {
		return nil, err
	}

	if err := types.UnmarshalAny(e.Response, msg); err != nil {
		return nil, err
	}

	resp, ok := msg.(Response)
	if !ok {
		return nil, fmt.Errorf("bad cached type")
	}
	return resp, nil
}

func (m *Sample) GetTimestampMs() int64 {
	if m != nil {
		if m.Sample != nil {
			return m.Sample.TimestampMs
		} else if m.Histogram != nil {
			return m.Histogram.TimestampMs
		}
	}
	return 0
}
