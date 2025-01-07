package client

import (
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
)

// ChunksCount returns the number of chunks in response.
func (m *QueryStreamResponse) ChunksCount() int {
	if len(m.Chunkseries) == 0 {
		return 0
	}

	count := 0
	for _, entry := range m.Chunkseries {
		count += len(entry.Chunks)
	}
	return count
}

// ChunksSize returns the size of all chunks in the response.
func (m *QueryStreamResponse) ChunksSize() int {
	if len(m.Chunkseries) == 0 {
		return 0
	}

	size := 0
	for _, entry := range m.Chunkseries {
		for _, chunk := range entry.Chunks {
			size += chunk.Size()
		}
	}
	return size
}

func (m *QueryStreamResponse) SamplesCount() (count int) {
	for _, cs := range m.Chunkseries {
		for _, c := range cs.Chunks {
			switch c.Encoding {
			case int32(encoding.PrometheusXorChunk), int32(encoding.PrometheusHistogramChunk), int32(encoding.PrometheusFloatHistogramChunk):
				count += int(binary.BigEndian.Uint16(c.Data))
			}
		}
	}
	return
}

func (m *LabelMatcher) MatcherType() (labels.MatchType, error) {
	var t labels.MatchType
	switch m.Type {
	case EQUAL:
		t = labels.MatchEqual
	case NOT_EQUAL:
		t = labels.MatchNotEqual
	case REGEX_MATCH:
		t = labels.MatchRegexp
	case REGEX_NO_MATCH:
		t = labels.MatchNotRegexp
	default:
		return 0, errors.Errorf("unrecognized label matcher type %d", m.Type)
	}
	return t, nil
}
