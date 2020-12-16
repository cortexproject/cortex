package tenantfederation

import (
	"context"
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/tenant"
)

const (
	defaultTenantLabel         = "__tenant_id__"
	retainExistingPrefix       = "original_"
	originalDefaultTenantLabel = retainExistingPrefix + defaultTenantLabel
)

// NewQueryable returns a queryable that iterates through all the tenant IDs
// that are part of the request and aggregates the results from each tenant's
// Querier by sending of subsequent requests.
// The result contains a label tenantLabelName to identify the tenant ID that
// it originally resulted from.
// If the label tenantLabelName is already existing, its value is overwritten
// by the tenant ID and the previous value is exposed through a new label
// prefixed with "original_". This behaviour is not implemented recursively
func NewQueryable(upstream storage.Queryable) storage.Queryable {
	return &mergeQueryable{
		upstream: upstream,
	}
}

type mergeQueryable struct {
	upstream storage.Queryable
}

// Querier returns a new mergeQuerier, which aggregates results from multiple
// tenants into a single result.
func (m *mergeQueryable) Querier(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}

	if len(tenantIDs) <= 1 {
		return m.upstream.Querier(ctx, mint, maxt)
	}

	var queriers = make([]storage.Querier, len(tenantIDs))
	for pos, tenantID := range tenantIDs {
		q, err := m.upstream.Querier(
			user.InjectOrgID(ctx, tenantID),
			mint,
			maxt,
		)
		if err != nil {
			return nil, err
		}
		queriers[pos] = q
	}

	return &mergeQuerier{
		queriers:  queriers,
		tenantIDs: tenantIDs,
	}, nil
}

// mergeQuerier aggregates the results from underlying queriers and adds a
// label tenantLabelName to identify the tenant ID that the metric resulted
// from.
// If the label tenantLabelName is already existing, its value is
// overwritten by the tenant ID and the previous value is exposed through a new
// label prefixed with "original_". This behaviour is not implemented recursively
type mergeQuerier struct {
	queriers  []storage.Querier
	tenantIDs []string
}

// LabelValues returns all potential values for a label name.
// It is not safe to use the strings beyond the lifefime of the querier.
// For the label "tenantLabelName" it will return all the tenant IDs available.
// For the label "original_" + tenantLabelName it will return all the values
// of the underlying queriers for tenantLabelName.
func (m *mergeQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	if name == defaultTenantLabel {
		return m.tenantIDs, nil, nil
	}

	// ensure the name of a retained tenant id label gets handled under the
	// original label name
	if name == originalDefaultTenantLabel {
		name = defaultTenantLabel
	}

	return m.mergeDistinctStringSlice(func(q storage.Querier) ([]string, storage.Warnings, error) {
		return q.LabelValues(name)
	})
}

// LabelNames returns all the unique label names present in the underlying
// queriers. It also adds the defaultTenantLabel and if present in the original
// results the originalDefaultTenantLabel
func (m *mergeQuerier) LabelNames() ([]string, storage.Warnings, error) {
	labelNames, warnings, err := m.mergeDistinctStringSlice(func(q storage.Querier) ([]string, storage.Warnings, error) {
		return q.LabelNames()
	})
	if err != nil {
		return nil, nil, err
	}

	// check if the tenant label exists in the original result
	var tenantLabelExists bool
	labelPos := sort.SearchStrings(labelNames, defaultTenantLabel)
	if labelPos < len(labelNames) && labelNames[labelPos] == defaultTenantLabel {
		tenantLabelExists = true
	}

	labelToAdd := defaultTenantLabel

	// if defaultTenantLabel already exists, we need to add the
	// originalDefaultTenantLabel
	if tenantLabelExists {
		labelToAdd = originalDefaultTenantLabel
		labelPos = sort.SearchStrings(labelNames, labelToAdd)
	}

	// insert label at the correct position
	labelNames = append(labelNames, "")
	copy(labelNames[labelPos+1:], labelNames[labelPos:])
	labelNames[labelPos] = labelToAdd

	return labelNames, warnings, nil
}

type stringSliceFunc func(storage.Querier) ([]string, storage.Warnings, error)

// mergeDistinctStringSlice is aggregating results from stringSliceFunc calls
// on a querier. It removes duplicates and sorts the result. It doesn't require
// the output of the stringSliceFunc to be sorted, as results of LabelValues
// are not sorted.
//
// TODO: Consider running stringSliceFunc calls concurrently
func (m *mergeQuerier) mergeDistinctStringSlice(f stringSliceFunc) ([]string, storage.Warnings, error) {
	var warnings storage.Warnings
	resultMap := make(map[string]struct{})
	for pos, tenantID := range m.tenantIDs {
		result, resultWarnings, err := f(m.queriers[pos])
		if err != nil {
			return nil, nil, err
		}
		for _, e := range result {
			resultMap[e] = struct{}{}
		}
		for _, w := range resultWarnings {
			warnings = append(warnings, fmt.Errorf("error querying tenant id %s: %w", tenantID, w))
		}
	}

	var result []string
	for e := range resultMap {
		result = append(result, e)
	}
	sort.Strings(result)
	return result, warnings, nil
}

// Close releases the resources of the Querier.
func (m *mergeQuerier) Close() error {
	var errs prometheus.MultiError
	for pos, tenantID := range m.tenantIDs {
		if err := m.queriers[pos].Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close querier for tenant id %s: %w", tenantID, err))
		}
	}
	return errs.MaybeUnwrap()
}

// Select returns a set of series that matches the given label matchers. If the
// tenantLabelName is matched on it only considers those queriers matching. The
// forwaded labelSelector is not containing those that operate on tenantLabelName.
func (m *mergeQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	tenantIDsPos, filteredMatchers := filterValuesByMatchers(string(defaultTenantLabel), m.tenantIDs, matchers...)
	var seriesSets = make([]storage.SeriesSet, len(tenantIDsPos))
	for pos, posTenant := range tenantIDsPos {
		tenantID := m.tenantIDs[posTenant]
		seriesSets[pos] = &addLabelsSeriesSet{
			// TODO: Consider running Select calls concurrently
			upstream: m.queriers[posTenant].Select(sortSeries, hints, filteredMatchers...),
			labels: labels.Labels{
				{
					Name:  defaultTenantLabel,
					Value: tenantID,
				},
			},
		}
	}
	return storage.NewMergeSeriesSet(seriesSets, storage.ChainedSeriesMerge)
}

// filterValuesByMatchers applies matchers to inputed labelName and labelValue.
// The index of matched values is returned and also all label matcher not
// matching the labelName.
// In case a label matcher is set on a label confliciting with tenantLabelName,
// we need to rename this labelMatcher to it's original name.
// This is used to as part of Select in the mergeQueryable, to ensure only
// relevant queries are considered and the forwarded matchers do not contain
// matchers on the tenantLabelName.
func filterValuesByMatchers(labelName string, labelValues []string, matchers ...*labels.Matcher) ([]int, []*labels.Matcher) {
	// this contains the matchers which are not related to labelName
	var unrelatedMatchers []*labels.Matcher

	// this contains the pos of labelValues that are matched by the matchers
	var matchedValuesPos = make([]int, len(labelValues))
	for pos := range labelValues {
		matchedValuesPos[pos] = pos
	}

	for _, m := range matchers {
		if m.Name != labelName {
			// check if has the retained label name
			if m.Name == originalDefaultTenantLabel {
				unrelatedMatchers = append(unrelatedMatchers, &labels.Matcher{
					Name:  labelName,
					Type:  m.Type,
					Value: m.Value,
				})
			} else {
				unrelatedMatchers = append(unrelatedMatchers, m)
			}
			continue
		}

		var matchedValuesPosThisMatcher []int
		for _, v := range matchedValuesPos {
			if m.Matches(labelValues[v]) {
				matchedValuesPosThisMatcher = append(matchedValuesPosThisMatcher, v)
			}
		}
		matchedValuesPos = matchedValuesPosThisMatcher
	}
	return matchedValuesPos, unrelatedMatchers
}

type addLabelsSeriesSet struct {
	upstream storage.SeriesSet
	labels   labels.Labels
}

func (m *addLabelsSeriesSet) Next() bool {
	return m.upstream.Next()
}

// At returns full series. Returned series should be iteratable even after Next is called.
func (m *addLabelsSeriesSet) At() storage.Series {
	return &addLabelsSeries{
		upstream: m.upstream.At(),
		labels:   m.labels,
	}
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (m *addLabelsSeriesSet) Err() error {
	return m.upstream.Err()
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (m *addLabelsSeriesSet) Warnings() storage.Warnings {
	return m.upstream.Warnings()
}

type addLabelsSeries struct {
	upstream storage.Series
	labels   labels.Labels
}

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (a *addLabelsSeries) Labels() labels.Labels {
	return setLabelsRetainExisting(a.upstream.Labels(), a.labels...)
}

// Iterator returns a new, independent iterator of the data of the series.
func (a *addLabelsSeries) Iterator() chunkenc.Iterator {
	return a.upstream.Iterator()
}

// this sets a label and preserves an existing value a new label prefixed with
// original_. It doesn't do this recursively.
func setLabelRetainExisting(ls labels.Labels, lb *labels.Builder, additional labels.Label) {
	if oldValue := ls.Get(additional.Name); oldValue != "" {
		lb.Set(
			retainExistingPrefix+additional.Name,
			oldValue,
		)
	}
	lb.Set(additional.Name, additional.Value)
}

func setLabelsRetainExisting(src labels.Labels, additionalLabels ...labels.Label) labels.Labels {
	lb := labels.NewBuilder(src)

	for _, additionalL := range additionalLabels {
		setLabelRetainExisting(src, lb, additionalL)
	}

	return lb.Labels()
}
