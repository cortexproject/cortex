package ring

import (
	"fmt"
	"html/template"
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
						<th>Actions</th>
					</tr>
				</thead>
				<tbody>
					{{ range $key, $value := .Ring.Ingesters }}
					<tr>
						<td>{{ $key }}</td>
						<td>{{ $value.State }}</td>
						<td>{{ $value.Hostname }}</td>
						<td>{{ $value.Timestamp | time }}</td>
						<td><button name="forget" value="{{ $key }}" type="submit">Forget</button></td>
					</tr>
					{{ end }}
				</tbody>
			</table>
		</form>
	</body>
</html>`

var tmpl *template.Template

func init() {
	var err error
	tmpl, err = template.New("webpage").
		Funcs(template.FuncMap{
			"time": func(in interface{}) string {
				return time.Unix(in.(int64), 0).String()
			},
		}).
		Parse(tpl)
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
	return r.consul.CAS(consulKey, unregister)
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
	if err := tmpl.Execute(w, struct {
		Ring    *Desc
		Message string
		Now     time.Time
	}{
		Ring:    r.ringDesc,
		Message: message,
		Now:     time.Now(),
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
