package distributor

import (
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
)

const trackerTpl = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Cortex HA Tracker Status</title>
	</head>
	<body>
		<h1>Cortex HA Tracker Status</h1>
		<p>Current time: {{ .Now }}</p>
		<table width="100%" border="1">
			<thead>
				<tr>
					<th>User ID</th>
					<th>Cluster</th>
					<th>Replica</th>
					<th>Elected Time</th>
					<th>Time Until Update</th>
					<th>Time Until Failover</th>
				</tr>
			</thead>
			<tbody>
				{{ range .Elected }}
				<tr>
					<td>{{ .UserID }}</td>
					<td>{{ .Cluster }}</td>
					<td>{{ .Replica }}</td>
					<td>{{ .ElectedAt }}</td>
					<td>{{ .UpdateTime }}</td>
					<td>{{ .FailoverTime }}</td>
				</tr>
				{{ end }}
			</tbody>
		</table>
	</body>
</html>`

var trackerTmpl *template.Template

func init() {
	trackerTmpl = template.Must(template.New("ha-tracker").Parse(trackerTpl))
}

func (h *haTracker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.electedLock.RLock()
	defer h.electedLock.RUnlock()

	type replica struct {
		UserID, Cluster, Replica string
		ElectedAt                time.Time
		UpdateTime, FailoverTime time.Duration
	}

	electedReplicas := []interface{}{}
	for key, desc := range h.elected {
		chunks := strings.SplitN(key, "/", 2)

		electedReplicas = append(electedReplicas, replica{
			UserID:       chunks[0],
			Cluster:      chunks[1],
			Replica:      desc.Replica,
			ElectedAt:    timestamp.Time(desc.ReceivedAt),
			UpdateTime:   time.Until(timestamp.Time(desc.ReceivedAt).Add(h.cfg.UpdateTimeout)),
			FailoverTime: time.Until(timestamp.Time(desc.ReceivedAt).Add(h.cfg.FailoverTimeout)),
		})
	}

	sort.Slice(electedReplicas, func(i, j int) bool {
		first := electedReplicas[i].(replica)
		second := electedReplicas[j].(replica)

		if first.UserID == second.UserID {
			return first.Cluster < second.Cluster
		}
		return first.UserID < second.UserID
	})

	if err := trackerTmpl.Execute(w, struct {
		Elected []interface{}
		Now     time.Time
	}{
		Elected: electedReplicas,
		Now:     time.Now(),
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
