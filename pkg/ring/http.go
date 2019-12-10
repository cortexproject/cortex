package ring

import (
	"context"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"

	"github.com/cortexproject/cortex/pkg/util"
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
		<form action="" method="POST">
			<input type="hidden" name="csrf_token" value="$__CSRF_TOKEN_PLACEHOLDER__">
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
					{{ range $i, $ing := .Ingesters }}
					{{ if mod $i 2 }}
					<tr>
					{{ else }}
					<tr bgcolor="#BEBEBE">
					{{ end }}
						<td>{{ .ID }}</td>
						<td>{{ .State }}</td>
						<td>{{ .Address }}</td>
						<td>{{ .Timestamp }}</td>
						<td>{{ .Tokens }}</td>
						<td>{{ .Ownership }}%</td>
						<td><button name="forget" value="{{ .ID }}" type="submit">Forget</button></td>
					</tr>
					{{ end }}
				</tbody>
			</table>
			<br>
			{{ if .ShowTokens }}
			<input type="button" value="Hide Ingester Tokens" onclick="window.location.href = '?tokens=false' " />
			{{ else }}
			<input type="button" value="Show Ingester Tokens" onclick="window.location.href = '?tokens=true'" />
			{{ end }}
			<pre>{{ .Ring }}</pre>
		</form>
	</body>
</html>`

var tmpl *template.Template

func init() {
	t := template.New("webpage")
	t.Funcs(template.FuncMap{"mod": func(i, j int) bool { return i%j == 0 }})
	tmpl = template.Must(t.Parse(tpl))
}

func (r *Ring) forget(ctx context.Context, id string) error {
	unregister := func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*Desc)
		ringDesc.RemoveIngester(id)
		return ringDesc, true, nil
	}
	return r.KVClient.CAS(ctx, ConsulKey, unregister)
}

func (r *Ring) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		ingesterID := req.FormValue("forget")
		if err := r.forget(req.Context(), ingesterID); err != nil {
			level.Error(util.WithContext(req.Context(), util.Logger)).Log("msg", "error forgetting ingester", "err", err)
		}

		// Implement PRG pattern to prevent double-POST and work with CSRF middleware.
		// https://en.wikipedia.org/wiki/Post/Redirect/Get

		// http.Redirect() would convert our relative URL to absolute, which is not what we want.
		// Browser knows how to do that, and it also knows real URL. Furthermore it will also preserve tokens parameter.
		// Note that relative Location URLs are explicitely allowed by specification, so we're not doing anything wrong here.
		w.Header().Set("Location", "#")
		w.WriteHeader(http.StatusFound)

		return
	}

	r.mtx.RLock()
	defer r.mtx.RUnlock()

	ingesterIDs := []string{}
	for id := range r.ringDesc.Ingesters {
		ingesterIDs = append(ingesterIDs, id)
	}
	sort.Strings(ingesterIDs)

	ingesters := []interface{}{}
	tokens, owned := countTokens(r.ringDesc)
	for _, id := range ingesterIDs {
		ing := r.ringDesc.Ingesters[id]
		timestamp := time.Unix(ing.Timestamp, 0)
		state := ing.State.String()
		if !r.IsHealthy(&ing, Reporting) {
			state = unhealthy
		}

		ingesters = append(ingesters, struct {
			ID, State, Address, Timestamp string
			Tokens                        uint32
			Ownership                     float64
		}{
			ID:        id,
			State:     state,
			Address:   ing.Addr,
			Timestamp: timestamp.String(),
			Tokens:    tokens[id],
			Ownership: (float64(owned[id]) / float64(math.MaxUint32)) * 100,
		})
	}

	tokensParam := req.URL.Query().Get("tokens")
	var ringDescString string
	showTokens := false
	if tokensParam == "true" {
		ringDescString = proto.MarshalTextString(r.ringDesc)
		showTokens = true
	}
	if err := tmpl.Execute(w, struct {
		Ingesters  []interface{}
		Now        time.Time
		Ring       string
		ShowTokens bool
	}{
		Ingesters:  ingesters,
		Now:        time.Now(),
		Ring:       ringDescString,
		ShowTokens: showTokens,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
