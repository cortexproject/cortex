package blocksconvert

type PlanHeader struct {
	User     string `json:"user"`
	DayIndex int    `json:"day_index"`
}

type PlanEntry struct {
	SeriesID string   `json:"sid"`
	Chunks   []string `json:"cs"`
}

type PlanFooter struct {
	Complete bool `json:"complete"`
}
