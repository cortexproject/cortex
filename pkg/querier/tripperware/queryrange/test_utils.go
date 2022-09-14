package queryrange

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
)

// genLabels will create a slice of labels where each label has an equal chance to occupy a value from [0,labelBuckets]. It returns a slice of length labelBuckets^len(labelSet)
func genLabels(
	labelSet []string,
	labelBuckets int,
) (result []labels.Labels) {
	if len(labelSet) == 0 {
		return result
	}

	l := labelSet[0]
	rest := genLabels(labelSet[1:], labelBuckets)

	for i := 0; i < labelBuckets; i++ {
		x := labels.Label{
			Name:  l,
			Value: fmt.Sprintf("%d", i),
		}
		if len(rest) == 0 {
			set := labels.Labels{x}
			result = append(result, set)
			continue
		}
		for _, others := range rest {
			set := append(others, x)
			result = append(result, set)
		}
	}
	return result

}
