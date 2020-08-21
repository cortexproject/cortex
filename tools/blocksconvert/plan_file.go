package blocksconvert

// Plan file describes which series must be included in a block for given user and day.
// It consists of JSON objects, each written on its own line.
// Plan file starts with single header, many plan entries and single footer.

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
