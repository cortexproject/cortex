package client

import (
	"encoding/binary"

	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/prometheus/prometheus/model/labels"
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

func (m *QueryStreamResponse) DesymbolizeLabels() {
	if len(m.Symbols) == 0 {
		return
	}

	b := labels.NewScratchBuilder(0)

	for i, cs := range m.Chunkseries {
		if len(cs.LabelsRefs) > 0 {
			m.Chunkseries[i].Labels = cortexpb.FromLabelsToLabelAdapters(desymbolizeLabels(&b, cs.LabelsRefs, m.Symbols))
		}
		cs.LabelsRefs = cs.LabelsRefs[:0]
		b.Reset()
	}
	m.Symbols = m.Symbols[:0]
}

func (m *QueryStreamResponse) SymbolizeLabels() {
	if len(m.Symbols) > 0 {
		return
	}

	st := writev2.NewSymbolTable()
	for i, _ := range m.Chunkseries {
		if len(m.Chunkseries[i].Labels) > 0 {
			m.Chunkseries[i].LabelsRefs = st.SymbolizeLabels(cortexpb.FromLabelAdaptersToLabels(m.Chunkseries[i].Labels), m.Chunkseries[i].LabelsRefs)
		}
		m.Chunkseries[i].Labels = m.Chunkseries[i].Labels[:0]
	}
	m.Symbols = st.Symbols()
}

// desymbolizeLabels decodes label references, with given symbols to labels.
func desymbolizeLabels(b *labels.ScratchBuilder, labelRefs []uint32, symbols []string) labels.Labels {
	b.Reset()
	for i := 0; i < len(labelRefs); i += 2 {
		b.Add(symbols[labelRefs[i]], symbols[labelRefs[i+1]])
	}
	b.Sort()
	return b.Labels()
}
