package blocksconvert

type PlanEntry struct {
	SeriesID string   `json:"sid"`
	Chunks   []string `json:"cs"`
}
