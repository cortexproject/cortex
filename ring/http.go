package ring

import (
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"time"
)

const tpl = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Cortex Ring Status</title>
	</head>
	<body>
		<h1>Cortex Ring Status</h1>
		<p>Current time: {{ .Now }}</p>
		<p>{{ .Message }}</p>
		<form action="" method="POST">
			<table width="100%" border="1">
				<thead>
					<tr>
						<th>Ingester</th>
						<th>State</th>
						<th>Address</th>
						<th>Last Heartbeat</th>
						<th>Tokens</th>
						<th>Ownership</th>
						<th>Actions</th>
					</tr>
				</thead>
				<tbody>
					{{ range .Ingesters }}
					<tr>
						<td>{{ .ID }}</td>
						<td>{{ .State }}</td>
						<td>{{ .Hostname }}</td>
						<td>{{ .Timestamp }}</td>
						<td>{{ .Tokens }}</td>
						<td>{{ .Ownership }}%</td>
						<td><button name="forget" value="{{ .ID }}" type="submit">Forget</button></td>
					</tr>
					{{ end }}
				</tbody>
			</table>
			<pre>{{ .Ring }}</pre>
		</form>
	</body>
</html>`

var tmpl *template.Template

func init() {
	var err error
	tmpl, err = template.New("webpage").Parse(tpl)
	if err != nil {
		panic(err)
	}
}

func (r *Ring) forget(id string) error {
	unregister := func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*Desc)
		ringDesc.removeIngester(id)
		return ringDesc, true, nil
	}
	return r.consul.CAS(consulKey, descFactory, unregister)
}

func (r *Ring) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	message := ""
	if req.Method == http.MethodPost {
		ingesterID := req.FormValue("forget")
		if err := r.forget(ingesterID); err != nil {
			message = fmt.Sprintf("Error forgetting ingester: %v", err)
		} else {
			message = fmt.Sprintf("Ingester %s forgotten", ingesterID)
		}
	}

	r.mtx.RLock()
	defer r.mtx.RUnlock()

	now := time.Now()
	ingesters := []interface{}{}
	tokens, owned := countTokens(r.ringDesc.Tokens)
	for id, ing := range r.ringDesc.Ingesters {
		state := ing.State.String()
		if now.Sub(ing.Timestamp) > r.heartbeatTimeout {
			state = unhealthy
		}

		ingesters = append(ingesters, struct {
			ID, State, Hostname, Timestamp string
			Tokens                         uint32
			Ownership                      float64
		}{
			ID:        id,
			State:     state,
			Hostname:  ing.Hostname,
			Timestamp: ing.Timestamp.String(),
			Tokens:    tokens[id],
			Ownership: (float64(owned[id]) / float64(math.MaxUint32)) * 100,
		})
	}

	buf, err := json.MarshalIndent(r.ringDesc, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tmpl.Execute(w, struct {
		Ingesters []interface{}
		Message   string
		Now       time.Time
		Ring      string
	}{
		Ingesters: ingesters,
		Message:   message,
		Now:       time.Now(),
		Ring:      string(buf),
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
