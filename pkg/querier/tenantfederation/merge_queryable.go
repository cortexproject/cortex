package tenantfederation

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

const (
	defaultTenantLabel   = "__tenant_id__"
	retainExistingPrefix = "original_"
	maxConcurrency       = 16
)

// NewQueryable returns a queryable that iterates through all the tenant IDs
// that are part of the request and aggregates the results from each tenant's
// Querier by sending of subsequent requests.
// By setting byPassWithSingleQuerier to true the mergeQuerier gets by-passed
// and results for request with a single querier will not contain the
// "__tenant_id__" label. This allows a smoother transition, when enabling
// tenant federation in a cluster.
// The result contains a label "__tenant_id__" to identify the tenant ID that
// it originally resulted from.
// If the label "__tenant_id__" is already existing, its value is overwritten
// by the tenant ID and the previous value is exposed through a new label
// prefixed with "original_". This behaviour is not implemented recursively.
func NewQueryable(upstream storage.Queryable, byPassWithSingleQuerier bool) storage.Queryable {
	return NewMergeQueryable(defaultTenantLabel, tenantQuerierCallback(upstream), byPassWithSingleQuerier)
}

func tenantQuerierCallback(queryable storage.Queryable) MergeQuerierCallback {
	return func(ctx context.Context, mint int64, maxt int64) ([]string, []storage.Querier, error) {
		tenantIDs, err := tenant.TenantIDs(ctx)
		if err != nil {
			return nil, nil, err
		}

		var queriers = make([]storage.Querier, len(tenantIDs))
		for pos := range tenantIDs {
			q, err := queryable.Querier(
				mint,
				maxt,
			)
			if err != nil {
				return nil, nil, err
			}
			queriers[pos] = q
		}

		return tenantIDs, queriers, nil
	}
}

// MergeQuerierCallback returns the underlying queriers and their IDs relevant
// for the query.
type MergeQuerierCallback func(ctx context.Context, mint int64, maxt int64) (ids []string, queriers []storage.Querier, err error)

// NewMergeQueryable returns a queryable that merges results from multiple
// underlying Queryables. The underlying queryables and its label values to be
// considered are returned by a MergeQuerierCallback.
// By setting byPassWithSingleQuerier to true the mergeQuerier gets by-passed
// and results for request with a single querier will not contain the id label.
// This allows a smoother transition, when enabling tenant federation in a
// cluster.
// Results contain a label `idLabelName` to identify the underlying queryable
// that it originally resulted from.
// If the label `idLabelName` is already existing, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively.
func NewMergeQueryable(idLabelName string, callback MergeQuerierCallback, byPassWithSingleQuerier bool) storage.Queryable {
	return &mergeQueryable{
		idLabelName:             idLabelName,
		callback:                callback,
		byPassWithSingleQuerier: byPassWithSingleQuerier,
	}
}

type mergeQueryable struct {
	idLabelName             string
	byPassWithSingleQuerier bool
	callback                MergeQuerierCallback
}

// Querier returns a new mergeQuerier, which aggregates results from multiple
// underlying queriers into a single result.
func (m *mergeQueryable) Querier(mint int64, maxt int64) (storage.Querier, error) {
	return &mergeQuerier{
		idLabelName:             m.idLabelName,
		mint:                    mint,
		maxt:                    maxt,
		byPassWithSingleQuerier: m.byPassWithSingleQuerier,
		callback:                m.callback,
	}, nil
}

// mergeQuerier aggregates the results from underlying queriers and adds a
// label `idLabelName` to identify the queryable that the metric resulted
// from.
// If the label `idLabelName` is already existing, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively
type mergeQuerier struct {
	idLabelName string
	mint, maxt  int64
	callback    MergeQuerierCallback

	byPassWithSingleQuerier bool
}

// LabelValues returns all potential values for a label name.  It is not safe
// to use the strings beyond the lifefime of the querier.
// For the label `idLabelName` it will return all the underlying ids available.
// For the label "original_" + `idLabelName it will return all the values
// of the underlying queriers for `idLabelName`.
func (m *mergeQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ids, queriers, err := m.callback(ctx, m.mint, m.maxt)
	if err != nil {
		return nil, nil, err
	}

	// by pass when only single querier is returned
	if m.byPassWithSingleQuerier && len(queriers) == 1 {
		return queriers[0].LabelValues(ctx, name, matchers...)
	}
	log, _ := spanlogger.New(ctx, "mergeQuerier.LabelValues")
	defer log.Span.Finish()

	matchedTenants, filteredMatchers := filterValuesByMatchers(m.idLabelName, ids, matchers...)

	if name == m.idLabelName {
		var labelValues = make([]string, 0, len(matchedTenants))
		for _, id := range ids {
			if _, matched := matchedTenants[id]; matched {
				labelValues = append(labelValues, id)
			}
		}
		return labelValues, nil, nil
	}

	// ensure the name of a retained label gets handled under the original
	// label name
	if name == retainExistingPrefix+m.idLabelName {
		name = m.idLabelName
	}

	return m.mergeDistinctStringSliceWithTenants(ctx, func(ctx context.Context, q storage.Querier) ([]string, annotations.Annotations, error) {
		return q.LabelValues(ctx, name, filteredMatchers...)
	}, matchedTenants, ids, queriers)
}

// LabelNames returns all the unique label names present in the underlying
// queriers. It also adds the `idLabelName` and if present in the original
// results the original `idLabelName`.
func (m *mergeQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ids, queriers, err := m.callback(ctx, m.mint, m.maxt)
	if err != nil {
		return nil, nil, err
	}

	// by pass when only single querier is returned
	if m.byPassWithSingleQuerier && len(queriers) == 1 {
		return queriers[0].LabelNames(ctx, matchers...)
	}
	log, _ := spanlogger.New(ctx, "mergeQuerier.LabelNames")
	defer log.Span.Finish()

	matchedTenants, filteredMatchers := filterValuesByMatchers(m.idLabelName, ids, matchers...)

	labelNames, warnings, err := m.mergeDistinctStringSliceWithTenants(ctx, func(ctx context.Context, q storage.Querier) ([]string, annotations.Annotations, error) {
		return q.LabelNames(ctx, filteredMatchers...)
	}, matchedTenants, ids, queriers)
	if err != nil {
		return nil, nil, err
	}

	// check if the `idLabelName` exists in the original result
	var idLabelNameExists bool
	labelPos := sort.SearchStrings(labelNames, m.idLabelName)
	if labelPos < len(labelNames) && labelNames[labelPos] == m.idLabelName {
		idLabelNameExists = true
	}

	labelToAdd := m.idLabelName

	// if `idLabelName` already exists, we need to add the name prefix with
	// retainExistingPrefix.
	if idLabelNameExists {
		labelToAdd = retainExistingPrefix + m.idLabelName
		labelPos = sort.SearchStrings(labelNames, labelToAdd)
	}

	// insert label at the correct position
	labelNames = append(labelNames, "")
	copy(labelNames[labelPos+1:], labelNames[labelPos:])
	labelNames[labelPos] = labelToAdd

	return labelNames, warnings, nil
}

type stringSliceFunc func(context.Context, storage.Querier) ([]string, annotations.Annotations, error)

type stringSliceFuncJob struct {
	querier  storage.Querier
	id       string
	result   []string
	warnings annotations.Annotations
}

// mergeDistinctStringSliceWithTenants aggregates stringSliceFunc call
// results from queriers whose tenant ids match the tenants map. If a nil map is
// provided, all queriers are used. It removes duplicates and sorts the result.
// It doesn't require the output of the stringSliceFunc to be sorted, as results
// of LabelValues are not sorted.
func (m *mergeQuerier) mergeDistinctStringSliceWithTenants(ctx context.Context, f stringSliceFunc, tenants map[string]struct{}, ids []string, queriers []storage.Querier) ([]string, annotations.Annotations, error) {
	var jobs []interface{}

	for pos, id := range ids {
		if tenants != nil {
			if _, matched := tenants[id]; !matched {
				continue
			}
		}

		jobs = append(jobs, &stringSliceFuncJob{
			querier: queriers[pos],
			id:      ids[pos],
		})
	}

	parentCtx := ctx
	run := func(ctx context.Context, jobIntf interface{}) error {
		job, ok := jobIntf.(*stringSliceFuncJob)
		if !ok {
			return fmt.Errorf("unexpected type %T", jobIntf)
		}

		var err error
		// Based on parent ctx here as we are using lazy querier.
		newCtx := user.InjectOrgID(parentCtx, job.id)
		job.result, job.warnings, err = f(newCtx, job.querier)
		if err != nil {
			return errors.Wrapf(err, "error querying %s %s", rewriteLabelName(m.idLabelName), job.id)
		}

		return nil
	}

	err := concurrency.ForEach(ctx, jobs, maxConcurrency, run)
	if err != nil {
		return nil, nil, err
	}

	// aggregate warnings and deduplicate string results
	var warnings annotations.Annotations
	resultMap := make(map[string]struct{})
	for _, jobIntf := range jobs {
		job, ok := jobIntf.(*stringSliceFuncJob)
		if !ok {
			return nil, nil, fmt.Errorf("unexpected type %T", jobIntf)
		}

		for _, e := range job.result {
			resultMap[e] = struct{}{}
		}

		for _, w := range job.warnings {
			warnings.Add(errors.Wrapf(w, "warning querying %s %s", rewriteLabelName(m.idLabelName), job.id))
		}
	}

	var result = make([]string, 0, len(resultMap))
	for e := range resultMap {
		result = append(result, e)
	}
	sort.Strings(result)
	return result, warnings, nil
}

// Close releases the resources of the Querier.
func (m *mergeQuerier) Close() error {
	return nil
}

type selectJob struct {
	pos     int
	querier storage.Querier
	id      string
}

// Select returns a set of series that matches the given label matchers. If the
// `idLabelName` is matched on, it only considers those queriers
// matching. The forwarded labelSelector is not containing those that operate
// on `idLabelName`.
func (m *mergeQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	ids, queriers, err := m.callback(ctx, m.mint, m.maxt)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// by pass when only single querier is returned
	if m.byPassWithSingleQuerier && len(queriers) == 1 {
		return queriers[0].Select(ctx, sortSeries, hints, matchers...)
	}

	log, ctx := spanlogger.New(ctx, "mergeQuerier.Select")
	defer log.Span.Finish()
	matchedValues, filteredMatchers := filterValuesByMatchers(m.idLabelName, ids, matchers...)
	var jobs = make([]interface{}, len(matchedValues))
	var seriesSets = make([]storage.SeriesSet, len(matchedValues))
	var jobPos int
	for labelPos := range ids {
		if _, matched := matchedValues[ids[labelPos]]; !matched {
			continue
		}
		jobs[jobPos] = &selectJob{
			pos:     jobPos,
			querier: queriers[labelPos],
			id:      ids[labelPos],
		}
		jobPos++
	}

	parentCtx := ctx
	run := func(ctx context.Context, jobIntf interface{}) error {
		job, ok := jobIntf.(*selectJob)
		if !ok {
			return fmt.Errorf("unexpected type %T", jobIntf)
		}
		// Based on parent ctx here as we are using lazy querier.
		newCtx := user.InjectOrgID(parentCtx, job.id)
		seriesSets[job.pos] = &addLabelsSeriesSet{
			upstream: job.querier.Select(newCtx, sortSeries, hints, filteredMatchers...),
			labels: labels.Labels{
				{
					Name:  m.idLabelName,
					Value: job.id,
				},
			},
		}
		return nil
	}

	if err := concurrency.ForEach(ctx, jobs, maxConcurrency, run); err != nil {
		return storage.ErrSeriesSet(err)
	}

	return storage.NewMergeSeriesSet(seriesSets, storage.ChainedSeriesMerge)
}

// filterValuesByMatchers applies matchers to inputed `idLabelName` and
// `ids`. A map of matched values is returned and also all label matchers not
// matching the `idLabelName`.
// In case a label matcher is set on a label conflicting with `idLabelName`, we
// need to rename this labelMatcher's name to its original name. This is used
// to as part of Select in the mergeQueryable, to ensure only relevant queries
// are considered and the forwarded matchers do not contain matchers on the
// `idLabelName`.
func filterValuesByMatchers(idLabelName string, ids []string, matchers ...*labels.Matcher) (matchedIDs map[string]struct{}, unrelatedMatchers []*labels.Matcher) {
	// this contains the matchers which are not related to idLabelName
	unrelatedMatchers = make([]*labels.Matcher, 0, len(matchers))

	// build map of values to consider for the matchers
	matchedIDs = make(map[string]struct{}, len(ids))
	for _, value := range ids {
		matchedIDs[value] = struct{}{}
	}

	for _, m := range matchers {
		switch m.Name {
		// matcher has idLabelName to target a specific tenant(s)
		case idLabelName:
			for value := range matchedIDs {
				if !m.Matches(value) {
					delete(matchedIDs, value)
				}
			}

		// check if has the retained label name
		case retainExistingPrefix + idLabelName:
			// rewrite label to the original name, by copying matcher and
			// replacing the label name
			rewrittenM := *m
			rewrittenM.Name = idLabelName
			unrelatedMatchers = append(unrelatedMatchers, &rewrittenM)

		default:
			unrelatedMatchers = append(unrelatedMatchers, m)
		}
	}

	return matchedIDs, unrelatedMatchers
}

type addLabelsSeriesSet struct {
	upstream   storage.SeriesSet
	labels     labels.Labels
	currSeries storage.Series
}

func (m *addLabelsSeriesSet) Next() bool {
	m.currSeries = nil
	return m.upstream.Next()
}

// At returns full series. Returned series should be iteratable even after Next is called.
func (m *addLabelsSeriesSet) At() storage.Series {
	if m.currSeries == nil {
		upstream := m.upstream.At()
		m.currSeries = &addLabelsSeries{
			upstream: upstream,
			labels:   setLabelsRetainExisting(upstream.Labels(), m.labels...),
		}
	}
	return m.currSeries
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (m *addLabelsSeriesSet) Err() error {
	return errors.Wrapf(m.upstream.Err(), "error querying %s", labelsToString(m.labels))
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (m *addLabelsSeriesSet) Warnings() annotations.Annotations {
	upstream := m.upstream.Warnings()
	warnings := make(annotations.Annotations, len(upstream))
	for pos := range upstream {
		warnings[pos] = errors.Wrapf(upstream[pos], "warning querying %s", labelsToString(m.labels))
	}
	return warnings
}

// rewrite label name to be more readable in error output
func rewriteLabelName(s string) string {
	return strings.TrimRight(strings.TrimLeft(s, "_"), "_")
}

// this outputs a more readable error format
func labelsToString(labels labels.Labels) string {
	parts := make([]string, len(labels))
	for pos, l := range labels {
		parts[pos] = rewriteLabelName(l.Name) + " " + l.Value
	}
	return strings.Join(parts, ", ")
}

type addLabelsSeries struct {
	upstream storage.Series
	labels   labels.Labels
}

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (a *addLabelsSeries) Labels() labels.Labels {
	return a.labels
}

// Iterator returns a new, independent iterator of the data of the series.
func (a *addLabelsSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	return a.upstream.Iterator(it)
}

// this sets a label and preserves an existing value a new label prefixed with
// original_. It doesn't do this recursively.
func setLabelsRetainExisting(src labels.Labels, additionalLabels ...labels.Label) labels.Labels {
	lb := labels.NewBuilder(src)

	for _, additionalL := range additionalLabels {
		if oldValue := src.Get(additionalL.Name); oldValue != "" {
			lb.Set(
				retainExistingPrefix+additionalL.Name,
				oldValue,
			)
		}
		lb.Set(additionalL.Name, additionalL.Value)
	}

	return lb.Labels()
}
