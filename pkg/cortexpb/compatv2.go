package cortexpb

import (
	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

// ToWriteRequestV2 converts matched slices of Labels, Samples, and Histograms into a WriteRequest proto.
func ToWriteRequestV2(lbls []labels.Labels, samples []Sample, histograms []Histogram, metadata []MetadataV2, source SourceEnum, help ...string) *WriteRequestV2 {
	st := writev2.NewSymbolTable()
	labelRefs := make([][]uint32, 0, len(lbls))
	for _, lbl := range lbls {
		labelRefs = append(labelRefs, st.SymbolizeLabels(lbl, nil))
	}

	for _, s := range help {
		st.Symbolize(s)
	}

	symbols := st.Symbols()

	req := &WriteRequestV2{
		Timeseries: PreallocTimeseriesV2SliceFromPool(),
		Symbols:    symbols,
		Source:     source,
	}

	i := 0
	for i < len(samples) || i < len(histograms) || i < len(metadata) {
		ts := TimeseriesV2FromPool()
		ts.LabelsRefs = labelRefs[i]
		if i < len(samples) {
			ts.Samples = append(ts.Samples, samples[i])
		}
		if i < len(histograms) {
			ts.Histograms = append(ts.Histograms, histograms[i])
		}
		if i < len(metadata) {
			ts.Metadata = metadata[i]
		}
		i++
		req.Timeseries = append(req.Timeseries, PreallocTimeseriesV2{TimeSeriesV2: ts})
	}

	return req
}

func GetLabelRefsFromLabelAdapters(symbols []string, las []LabelAdapter) []uint32 {
	var ret []uint32

	symbolMap := map[string]uint32{}
	for idx, symbol := range symbols {
		symbolMap[symbol] = uint32(idx)
	}

	for _, lb := range las {
		if idx, ok := symbolMap[lb.Name]; ok {
			ret = append(ret, idx)
		}
		if idx, ok := symbolMap[lb.Value]; ok {
			ret = append(ret, idx)
		}
	}

	return ret
}

func GetLabelsRefsFromLabels(symbols []string, lbs labels.Labels) []uint32 {
	var ret []uint32

	symbolMap := map[string]uint32{}
	for idx, symbol := range symbols {
		symbolMap[symbol] = uint32(idx)
	}

	for _, lb := range lbs {
		if idx, ok := symbolMap[lb.Name]; ok {
			ret = append(ret, idx)
		}
		if idx, ok := symbolMap[lb.Value]; ok {
			ret = append(ret, idx)
		}
	}

	return ret
}
