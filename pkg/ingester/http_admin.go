package ingester

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
)

const tpl = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Cortex Ingester Stats</title>
	</head>
	<body>
		<h1>Cortex Ingester Stats</h1>
		<p>Current time: {{ .Now }}</p>
		{{if (gt .ReplicationFactor 0)}}
		<p><b>NB stats do not account for replication factor, which is currently set to {{ .ReplicationFactor }}</b></p>
		{{end}}
		<form action="" method="POST">
			<input type="hidden" name="csrf_token" value="$__CSRF_TOKEN_PLACEHOLDER__">
			<table border="1">
				<thead>
					<tr>
						<th>User</th>
						<th>Loaded Blocks</th>
						<th># Series</th>
						<th># Active Series</th>
						<th>Total Ingest Rate</th>
						<th>API Ingest Rate</th>
						<th>Rule Ingest Rate</th>
					</tr>
				</thead>
				<tbody>
					{{ range .Stats }}
					<tr>
						<td>{{ .UserID }}</td>
						<td align='right'>{{ .UserStats.LoadedBlocks }}</td>
						<td align='right'>{{ .UserStats.NumSeries }}</td>
						<td align='right'>{{ .UserStats.ActiveSeries }}</td>
						<td align='right'>{{ printf "%.2f" .UserStats.IngestionRate }}</td>
						<td align='right'>{{ printf "%.2f" .UserStats.APIIngestionRate }}</td>
						<td align='right'>{{ printf "%.2f" .UserStats.RuleIngestionRate }}</td>
					</tr>
					{{ end }}
				</tbody>
			</table>
		</form>
	</body>
</html>`

var UserStatsTmpl *template.Template

func init() {
	UserStatsTmpl = template.Must(template.New("webpage").Parse(tpl))
}

type UserStatsByTimeseries []UserIDStats

func (s UserStatsByTimeseries) Len() int      { return len(s) }
func (s UserStatsByTimeseries) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s UserStatsByTimeseries) Less(i, j int) bool {
	return s[i].NumSeries > s[j].NumSeries ||
		(s[i].NumSeries == s[j].NumSeries && s[i].UserID < s[j].UserID)
}

// UserIDStats models ingestion statistics for one user, including the user ID
type UserIDStats struct {
	UserID string `json:"userID"`
	UserStats
}

// UserStats models ingestion statistics for one user.
type UserStats struct {
	IngestionRate     float64 `json:"ingestionRate"`
	NumSeries         uint64  `json:"numSeries"`
	APIIngestionRate  float64 `json:"APIIngestionRate"`
	RuleIngestionRate float64 `json:"RuleIngestionRate"`
	ActiveSeries      uint64  `json:"activeSeries"`
	LoadedBlocks      uint64  `json:"loadedBlocks"`
}

// AllUserStatsRender render data for all users or return in json format.
func AllUserStatsRender(w http.ResponseWriter, r *http.Request, stats []UserIDStats, rf int) {
	sort.Sort(UserStatsByTimeseries(stats))

	if encodings, found := r.Header["Accept"]; found &&
		len(encodings) > 0 && strings.Contains(encodings[0], "json") {
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, fmt.Sprintf("Error marshalling response: %v", err), http.StatusInternalServerError)
		}
		return
	}

	util.RenderHTTPResponse(w, struct {
		Now               time.Time     `json:"now"`
		Stats             []UserIDStats `json:"stats"`
		ReplicationFactor int           `json:"replicationFactor"`
	}{
		Now:               time.Now(),
		Stats:             stats,
		ReplicationFactor: rf,
	}, UserStatsTmpl, r)
}
