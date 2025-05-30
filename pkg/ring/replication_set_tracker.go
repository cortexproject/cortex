package ring

import (
	"fmt"
)

type replicationSetResultTracker interface {
	// Signals an instance has done the execution, either successful (no error)
	// or failed (with error). If successful, result will be recorded and can
	// be accessed via getResults.
	done(instance *InstanceDesc, result interface{}, err error)

	// Returns true if all instances are done executing
	finished() bool

	// Returns true if the minimum number of successful results have been received.
	succeeded() bool

	// Returns true if the maximum number of failed executions have been reached.
	failed() bool

	// Returns true if executions failed in all instances or all zones.
	failedCompletely() bool

	// Returns recorded results.
	getResults() []interface{}

	// Returns errors
	getErrors() []error
}

type defaultResultTracker struct {
	minSucceeded int
	numSucceeded int
	numErrors    int
	maxErrors    int
	results      []interface{}
	numInstances int
	errors       []error
}

func newDefaultResultTracker(instances []InstanceDesc, maxErrors int) *defaultResultTracker {
	return &defaultResultTracker{
		minSucceeded: len(instances) - maxErrors,
		numSucceeded: 0,
		numErrors:    0,
		maxErrors:    maxErrors,
		errors:       make([]error, 0, len(instances)),
		results:      make([]interface{}, 0, len(instances)),
		numInstances: len(instances),
	}
}

func (t *defaultResultTracker) done(instance *InstanceDesc, result interface{}, err error) {
	if err == nil {
		t.numSucceeded++
		t.results = append(t.results, result)
	} else {
		t.errors = append(t.errors, fmt.Errorf("(%s) %w", instance.GetAddr(), err))
		t.numErrors++
	}
}

func (t *defaultResultTracker) finished() bool {
	return t.numSucceeded+t.numErrors == t.numInstances
}

func (t *defaultResultTracker) succeeded() bool {
	return t.numSucceeded >= t.minSucceeded
}

func (t *defaultResultTracker) failed() bool {
	return t.numErrors > t.maxErrors
}

func (t *defaultResultTracker) failedCompletely() bool {
	return t.numInstances == t.numErrors
}

func (t *defaultResultTracker) getResults() []interface{} {
	return t.results
}

func (t *defaultResultTracker) getErrors() []error {
	return t.errors
}

// zoneAwareResultTracker tracks the results per zone.
// All instances in a zone must succeed in order for the zone to succeed.
type zoneAwareResultTracker struct {
	waitingByZone       map[string]int
	failuresByZone      map[string]int
	minSuccessfulZones  int
	maxUnavailableZones int
	resultsPerZone      map[string][]interface{}
	numInstances        int
	zoneResultsQuorum   bool
	zoneCount           int
	doneCount           int
	errors              []error
}

func newZoneAwareResultTracker(instances []InstanceDesc, maxUnavailableZones int, zoneResultsQuorum bool) *zoneAwareResultTracker {
	t := &zoneAwareResultTracker{
		waitingByZone:       make(map[string]int),
		failuresByZone:      make(map[string]int),
		maxUnavailableZones: maxUnavailableZones,
		numInstances:        len(instances),
		zoneResultsQuorum:   zoneResultsQuorum,
		errors:              make([]error, 0, len(instances)),
	}

	for _, instance := range instances {
		t.waitingByZone[instance.Zone]++
	}
	t.minSuccessfulZones = len(t.waitingByZone) - maxUnavailableZones
	t.resultsPerZone = make(map[string][]interface{}, len(t.waitingByZone))
	t.zoneCount = len(t.waitingByZone)

	return t
}

func (t *zoneAwareResultTracker) done(instance *InstanceDesc, result interface{}, err error) {
	if err != nil {
		t.failuresByZone[instance.Zone]++
		t.errors = append(t.errors, fmt.Errorf("(%s, %s) %w", instance.GetAddr(), instance.GetZone(), err))
	} else {
		if _, ok := t.resultsPerZone[instance.Zone]; !ok {
			// If it is the first result in the zone, then total number of instances
			// in this zone should be number of waiting required.
			t.resultsPerZone[instance.Zone] = make([]interface{}, 0, t.waitingByZone[instance.Zone])
		}
		t.resultsPerZone[instance.Zone] = append(t.resultsPerZone[instance.Zone], result)
	}

	t.waitingByZone[instance.Zone]--
	t.doneCount++
}

func (t *zoneAwareResultTracker) finished() bool {
	return t.doneCount == t.numInstances
}

func (t *zoneAwareResultTracker) succeeded() bool {
	successfulZones := 0

	// The execution succeeded once we successfully received a successful result
	// from "all zones - max unavailable zones".
	for zone, numWaiting := range t.waitingByZone {
		if numWaiting == 0 && t.failuresByZone[zone] == 0 {
			successfulZones++
		}
	}

	return successfulZones >= t.minSuccessfulZones
}

func (t *zoneAwareResultTracker) failed() bool {
	failedZones := len(t.failuresByZone)
	return failedZones > t.maxUnavailableZones
}

func (t *zoneAwareResultTracker) failedCompletely() bool {
	failedZones := len(t.failuresByZone)
	allZonesFailed := failedZones == t.zoneCount
	atLeastHalfOfFleetFailed := len(t.errors) >= t.numInstances/2
	return allZonesFailed || (t.failed() && atLeastHalfOfFleetFailed)
}

func (t *zoneAwareResultTracker) getResults() []interface{} {
	results := make([]interface{}, 0, t.numInstances)
	if t.zoneResultsQuorum {
		for zone, waiting := range t.waitingByZone {
			// No need to check failuresByZone since tracker
			// should already succeed before reaching here.
			if waiting == 0 {
				results = append(results, t.resultsPerZone[zone]...)
			}
		}
	} else {
		for zone := range t.resultsPerZone {
			results = append(results, t.resultsPerZone[zone]...)
		}
	}
	return results
}

func (t *zoneAwareResultTracker) getErrors() []error {
	return t.errors
}
