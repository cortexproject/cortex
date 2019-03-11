package ingester

import (
	"sort"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/extract"
)

// A series is uniquely identified by its set of label name/value
// pairs, which may arrive in any order over the wire
type labelPairs []client.LabelPair

func labelsToMetric(s labels.Labels) model.Metric {
	metric := make(model.Metric, len(s))
	for _, l := range s {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return metric
}

var labelNameBytes = []byte(model.MetricNameLabel)

func (a labelPairs) String() string {
	var b strings.Builder

	metricName, err := extract.MetricNameFromLabelPairs(a)
	numLabels := len(a) - 1
	if err != nil {
		numLabels = len(a)
	}
	b.Write(metricName)
	b.WriteByte('{')
	count := 0
	for _, pair := range a {
		if !pair.Name.Equal(labelNameBytes) {
			b.Write(pair.Name)
			b.WriteString("=\"")
			b.Write(pair.Value)
			b.WriteByte('"')
			count++
			if count < numLabels {
				b.WriteByte(',')
			}
		}
	}
	b.WriteByte('}')
	return b.String()
}

// Remove any label where the value is "" - Prometheus 2+ will remove these
// before sending, but other clients such as Prometheus 1.x might send us blanks.
func (a *labelPairs) removeBlanks() {
	for i := 0; i < len(*a); {
		if len((*a)[i].Value) == 0 {
			// Delete by swap with the value at the end of the slice
			(*a)[i] = (*a)[len(*a)-1]
			(*a) = (*a)[:len(*a)-1]
			continue // go round and check the data that is now at position i
		}
		i++
	}
}

func (a labelPairs) copyValuesAndSort() labels.Labels {
	c := make(labels.Labels, len(a))
	// Since names and values may point into a much larger buffer,
	// make a copy of all the names and values, in one block for efficiency
	copyBytes := make([]byte, 0, len(a)*32) // guess at initial length
	for _, pair := range a {
		copyBytes = append(copyBytes, pair.Name...)
		copyBytes = append(copyBytes, pair.Value...)
	}
	// Now we need to copy the byte slice into a string for the values to point into
	copyString := string(copyBytes)
	pos := 0
	stringSlice := func(val []byte) string {
		start := pos
		pos += len(val)
		return copyString[start:pos]
	}
	for i, pair := range a {
		c[i].Name = stringSlice(pair.Name)
		c[i].Value = stringSlice(pair.Value)
	}
	sort.Sort(c)
	return c
}

func valueForName(s labels.Labels, name []byte) (string, bool) {
	pos := sort.Search(len(s), func(i int) bool { return s[i].Name >= string(name) })
	if pos == len(s) || s[pos].Name != string(name) {
		return "", false
	}
	return s[pos].Value, true
}

// Check if a and b contain the same name/value pairs
func (a labelPairs) equal(b labels.Labels) bool {
	if len(a) != len(b) {
		return false
	}
	for _, pair := range a {
		v, found := valueForName(b, pair.Name)
		if !found || v != string(pair.Value) {
			return false
		}
	}
	return true
}
