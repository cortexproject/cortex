package distributor

import (
	"html/template"
	"net/http"
	"strings"
	"time"
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
		<form action="" method="POST">
			<input type="hidden" name="csrf_token" value="$__CSRF_TOKEN_PLACEHOLDER__">
			<table width="100%" border="1">
				<thead>
					<tr>
						<th>User ID</th>
						<th>Cluster</th>
						<th>Replica</th>
						<th>Timestamp</th>
					</tr>
				</thead>
				<tbody>
					{{ range .Elected }}
					<tr>
						<td>{{ .UserID }}</td>
						<td>{{ .Cluster }}</td>
						<td>{{ .Replica }}</td>
						<td>{{ .Timestamp }}</td>
					</tr>
					{{ end }}
				</tbody>
			</table>
		</form>
	</body>
</html>`

var trackerTmpl *template.Template

func init() {
	trackerTmpl = template.Must(template.New("ha-tracker").Parse(trackerTpl))
}

func (h *haTracker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.electedLock.RLock()
	defer h.electedLock.RUnlock()

	electedReplicas := []interface{}{}
	for key, desc := range h.elected {
		chunks := strings.SplitN(key, "/", 2)

		electedReplicas = append(electedReplicas, struct {
			UserID, Cluster, Replica string
			Timestamp                int64
		}{
			UserID:    chunks[0],
			Cluster:   chunks[1],
			Replica:   desc.Replica,
			Timestamp: desc.ReceivedAt,
		})
	}

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
