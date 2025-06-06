package analysis

type QueryTelemetry struct {
	OperatorName     string           `json:"name,omitempty"`
	Execution        string           `json:"executionTime,omitempty"`
	SeriesExecution  string           `json:"seriesExecutionTime,omitempty"`
	SamplesExecution string           `json:"samplesExecutionTime,omitempty"`
	Series           int              `json:"series,omitempty"`
	PeakSamples      int64            `json:"peakSamples,omitempty"`
	TotalSamples     int64            `json:"totalSamples,omitempty"`
	Children         []QueryTelemetry `json:"children,omitempty"`
}
