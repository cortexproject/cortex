package api

import (
	"html/template"
	"net/http"
	"path"

	"github.com/go-kit/kit/log/level"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util"
)

// TODO: Update this content to be a template that is dynamic based on how Cortex is run.
var indexPageContent = template.Must(template.New("main").Parse(`
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Cortex</title>
	</head>
	<body>
		<h1>Cortex</h1>
		<p>Admin Endpoints:</p>
		<ul>
			<li><a href="{{ .Join "/config" }}">Current Config</a></li>
			<li><a href="{{ .Join "/distributor/all_user_stats" }}">Usage Statistics</a></li>
			<li><a href="{{ .Join "/distributor/ha_tracker" }}">HA Tracking Status</a></li>
			<li><a href="{{ .Join "/multitenant_alertmanager/status" }}">Alertmanager Status</a></li>
			<li><a href="{{ .Join "/ingester/ring" }}">Ingester Ring Status</a></li>
			<li><a href="{{ .Join "/ruler/ring" }}">Ruler Ring Status</a></li>
			<li><a href="{{ .Join "/services" }}">Service Status</a></li>
			<li><a href="{{ .Join "/compactor/ring" }}">Compactor Ring Status (experimental blocks storage)</a></li>
			<li><a href="{{ .Join "/store-gateway/ring" }}">Store Gateway Ring (experimental blocks storage)</a></li>
		</ul>

		<p>Dangerous:</p>
		<ul>
			<li><a href="{{ .Join "/ingester/flush" }}">Trigger a Flush</a></li>
			<li><a href="{{ .Join "/ingester/shutdown" }}">Trigger Ingester Shutdown</a></li>
		</ul>
	</body>
</html>`))

type templateInp struct {
	Prefix string
}

func (i templateInp) Join(p string) string {
	return path.Join(i.Prefix, p)
}

func indexHandler(httpPathPrefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := indexPageContent.Execute(w, templateInp{Prefix: httpPathPrefix})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func configHandler(cfg interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		out, err := yaml.Marshal(cfg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/yaml")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(out); err != nil {
			level.Error(util.Logger).Log("msg", "error writing response", "err", err)
		}
	}
}
