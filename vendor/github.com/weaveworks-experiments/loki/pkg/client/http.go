package loki

import (
	"fmt"
	"html/template"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/weaveworks-experiments/loki/pkg/model"
)

func contains(haystack []string, needle string) bool {
	for _, hay := range haystack {
		if hay == needle {
			return true
		}
	}
	return false
}

func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	traces := c.gather()
	encodings := strings.Split(r.Header.Get("Accept"), ",")
	if contains(encodings, "text/html") {
		encodeHTML(traces, w, r)
	} else {
		encodeProto(traces, w)
	}
}

func Handler() http.Handler {
	return globalCollector
}

func encodeProto(ts []model.Trace, w http.ResponseWriter) {
	traces := model.Traces{
		Traces: ts,
	}
	buf, err := traces.Marshal()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(buf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

const tracesTpl = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Traces</title>
		<style type="text/css">
			td:nth-child(1) {
				width: 20%;
			}
			td:nth-child(2) {
				width: 20%;
			}
		</style>
	</head>
	<body>
		<h1>Traces</h1>
		<table width="100%" border="1">
			<thead>
				<tr>
					<th>Time</th>
					<th>Duration</th>
					<th>Operation</th>
				</tr>
			</thead>
			<tbody>
				{{ range .Traces }}
				<tr>
					<td>{{ .Start }}</td>
					<td>{{ .Duration }}</td>
					<td><a href="/traces/{{ .TraceId }}">{{ .OperationName }}</a></td>
				</tr>
				{{ end }}
			</tbody>
		</table>
	</body>
</html>`

const traceTpl = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Trace {{ .TraceId }}</title>
	</head>
	<body>
		<h1>Trace {{ .TraceId }}</h1>
		<pre>
		{{ range .Spans }}
{{ .Start }} {{ .End.Sub .Start }} {{ .OperationName }}
{{ range .Tags }}    {{ .Key }}: {{ .Value }}
{{ end }}
		{{ end }}
		</pre>
		</table>
	</body>
</html>`

var tracesTmpl *template.Template
var traceTmpl *template.Template

func init() {
	var err error
	tracesTmpl, err = template.New("webpage").Parse(tracesTpl)
	if err != nil {
		panic(err)
	}
	traceTmpl, err = template.New("webpage").Parse(traceTpl)
	if err != nil {
		panic(err)
	}
}

var pathRegexp = regexp.MustCompile("^/traces/([0-9]+)$")

type tracesByStart []model.Trace

func (a tracesByStart) Len() int           { return len(a) }
func (a tracesByStart) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a tracesByStart) Less(i, j int) bool { return a[i].Start().Before(a[j].Start()) }

type spansByStart []model.Span

func (a spansByStart) Len() int           { return len(a) }
func (a spansByStart) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a spansByStart) Less(i, j int) bool { return a[i].Start.Before(a[j].Start) }

func encodeHTML(traces []model.Trace, w http.ResponseWriter, r *http.Request) {
	if matches := pathRegexp.FindStringSubmatch(r.RequestURI); len(matches) == 2 {
		traceID, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var trace *model.Trace
		for _, t := range traces {
			if t.TraceId == traceID {
				trace = &t
				break
			}
		}
		if trace == nil {
			http.Error(w, fmt.Sprintf("Trace %d not found", traceID), http.StatusNotFound)
			return
		}
		sort.Sort(spansByStart(trace.Spans))
		if err := traceTmpl.Execute(w, trace); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		sort.Sort(sort.Reverse(tracesByStart(traces)))
		if err := tracesTmpl.Execute(w, struct {
			Traces []model.Trace
		}{
			Traces: traces,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
