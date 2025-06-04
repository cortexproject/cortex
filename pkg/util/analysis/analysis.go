package analysis

type QueryTelemetry struct {
	// TODO(saswatamcode): Replace with engine.TrackedTelemetry once it has exported fields.
	// TODO(saswatamcode): Add aggregate fields to enrich data.
	OperatorName string           `json:"name,omitempty"`
	Execution    string           `json:"executionTime,omitempty"`
	PeakSamples  int64            `json:"peakSamples,omitempty"`
	TotalSamples int64            `json:"totalSamples,omitempty"`
	Children     []QueryTelemetry `json:"children,omitempty"`
}
