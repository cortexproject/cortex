package blocksconvert

import (
	"compress/gzip"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
)

// Plan file describes which series must be included in a block for given user and day.
// It consists of JSON objects, each written on its own line.
// Plan file starts with single header, many plan entries and single footer.

type PlanEntry struct {
	// Header
	User     string `json:"user,omitempty"`
	DayIndex int    `json:"day_index,omitempty"`

	// Entries
	SeriesID string   `json:"sid,omitempty"`
	Chunks   []string `json:"cs,omitempty"`

	// Footer
	Complete bool `json:"complete,omitempty"`
}

func (pe *PlanEntry) Reset() {
	*pe = PlanEntry{}
}

// Returns true and "base name" or false and empty string.
func IsPlanFilename(name string) (bool, string) {
	switch {
	case strings.HasSuffix(name, ".plan.gz"):
		return true, name[:len(name)-len(".plan.gz")]

	case strings.HasSuffix(name, ".plan.snappy"):
		return true, name[:len(name)-len(".plan.snappy")]

	case strings.HasSuffix(name, ".plan"):
		return true, name[:len(name)-len(".plan")]
	}

	return false, ""
}

func PreparePlanFileReader(planFile string, in io.Reader) (io.Reader, error) {
	switch {
	case strings.HasSuffix(planFile, ".snappy"):
		return snappy.NewReader(in), nil

	case strings.HasSuffix(planFile, ".gz"):
		return gzip.NewReader(in)
	}

	return in, nil
}

func StartingFilename(planBaseName string, t time.Time) string {
	return fmt.Sprintf("%s.starting.%d", planBaseName, t.Unix())
}

func ProgressFilename(planBaseName string, t time.Time) string {
	return fmt.Sprintf("%s.inprogress.%d", planBaseName, t.Unix())
}

var progress = regexp.MustCompile(`^(.+)\.(starting|progress|inprogress)\.(\d+)$`)

func IsProgressFilename(name string) (bool, string, time.Time) {
	m := progress.FindStringSubmatch(name)
	if len(m) == 0 {
		return false, "", time.Time{}
	}

	ts, err := strconv.ParseInt(m[3], 10, 64)
	if err != nil {
		return false, "", time.Time{}
	}

	return true, m[1], time.Unix(ts, 0)
}

func FinishedFilename(planBaseName string, id string) string {
	return fmt.Sprintf("%s.finished.%s", planBaseName, id)
}

var finished = regexp.MustCompile(`^(.+)\.finished\.([a-zA-Z0-9]+)$`)

func IsFinishedFilename(name string) (bool, string, string) {
	m := finished.FindStringSubmatch(name)
	if len(m) == 0 {
		return false, "", ""
	}

	return true, m[1], m[2]
}

func ErrorFilename(planBaseName string) string {
	return planBaseName + ".error"
}

func IsErrorFilename(name string) (bool, string) {
	if strings.HasSuffix(name, ".error") {
		return true, name[:len(name)-len(".error")]
	}
	return false, ""
}
