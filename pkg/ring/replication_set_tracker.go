package ring

type replicationSetResultTracker interface {
	// Signals an instance has done the execution, either successful (no error)
	// or failed (with error).
	done(instance *IngesterDesc, err error)

	// Returns true if the minimum number of successful results have been received.
	succeeded() bool

	// Returns true if the maximum number of failed executions have been reached.
	failed() bool
}

type defaultResultTracker struct {
	minSucceeded int
	numSucceeded int
	numErrors    int
	maxErrors    int
}

func newDefaultResultTracker(instances []IngesterDesc, maxErrors int) *defaultResultTracker {
	return &defaultResultTracker{
		minSucceeded: len(instances) - maxErrors,
		numSucceeded: 0,
		numErrors:    0,
		maxErrors:    maxErrors,
	}
}

func (t *defaultResultTracker) done(_ *IngesterDesc, err error) {
	if err == nil {
		t.numSucceeded++
	} else {
		t.numErrors++
	}
}

func (t *defaultResultTracker) succeeded() bool {
	return t.numSucceeded >= t.minSucceeded
}

func (t *defaultResultTracker) failed() bool {
	return t.numErrors > t.maxErrors
}

type zoneAwareResultTracker struct {
	waitingByZone       map[string]int
	failuresByZone      map[string]int
	minSuccessfulZones  int
	maxUnavailableZones int
}

func newZoneAwareResultTracker(instances []IngesterDesc, maxUnavailableZones int) *zoneAwareResultTracker {
	t := &zoneAwareResultTracker{
		waitingByZone:       make(map[string]int),
		failuresByZone:      make(map[string]int),
		maxUnavailableZones: maxUnavailableZones,
	}

	for _, instance := range instances {
		t.waitingByZone[instance.Zone]++
		t.failuresByZone[instance.Zone] = 0
	}
	t.minSuccessfulZones = len(t.waitingByZone) - maxUnavailableZones

	return t
}

func (t *zoneAwareResultTracker) done(instance *IngesterDesc, err error) {
	t.waitingByZone[instance.Zone]--

	if err != nil {
		t.failuresByZone[instance.Zone]++
	}
}

func (t *zoneAwareResultTracker) succeeded() bool {
	actualSucceededZones := 0

	// The execution succeeded once we successfully received a successful result
	// from "all zones - max unavailable zones".
	for zone, numWaiting := range t.waitingByZone {
		if numWaiting == 0 && t.failuresByZone[zone] == 0 {
			actualSucceededZones++
		}
	}

	return actualSucceededZones >= t.minSuccessfulZones
}

func (t *zoneAwareResultTracker) failed() bool {
	failedZones := 0

	// The execution failed if the number of zones, for which we have tracked at least 1
	// failure, exceeds the max unavailable zones.
	for _, numFailures := range t.failuresByZone {
		if numFailures > 0 {
			failedZones++
		}
	}

	return failedZones > t.maxUnavailableZones
}
