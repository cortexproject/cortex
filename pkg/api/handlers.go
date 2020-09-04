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
			<li><a href="{{ .JoinPath "/config" }}">Current Config</a></li>
			<li><a href="{{ .JoinPath "/distributor/all_user_stats" }}">Usage Statistics</a></li>
			<li><a href="{{ .JoinPath "/distributor/ha_tracker" }}">HA Tracking Status</a></li>
			<li><a href="{{ .JoinPath "/multitenant_alertmanager/status" }}">Alertmanager Status</a></li>
			<li><a href="{{ .JoinPath "/ingester/ring" }}">Ingester Ring Status</a></li>
			<li><a href="{{ .JoinPath "/ruler/ring" }}">Ruler Ring Status</a></li>
			<li><a href="{{ .JoinPath "/services" }}">Service Status</a></li>
			<li><a href="{{ .JoinPath "/compactor/ring" }}">Compactor Ring Status (experimental blocks storage)</a></li>
			<li><a href="{{ .JoinPath "/store-gateway/ring" }}">Store Gateway Ring (experimental blocks storage)</a></li>
		</ul>

		<p>Dangerous:</p>
		<ul>
			<li><a href="{{ .JoinPath "/ingester/flush" }}">Trigger a Flush</a></li>
			<li><a href="{{ .JoinPath "/ingester/shutdown" }}">Trigger Ingester Shutdown</a></li>
		</ul>
	</body>
</html>`))

type indexPageInput struct {
	pathPrefix string
}

func (i indexPageInput) JoinPath(p string) string {
	return path.Join(i.pathPrefix, p)
}

func indexHandler(httpPathPrefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := indexPageContent.Execute(w, indexPageInput{pathPrefix: httpPathPrefix})
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
