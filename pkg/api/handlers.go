package api

import (
	"net/http"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"gopkg.in/yaml.v2"
)

// TODO: Update this content to be a template that is dynamic based on how Cortex is run.
const indexPageContent = `
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
			<li><a href="/compactor/ring">Compactor Ring Status</a></li>
			<li><a href="/config">Current Config</a></li>
			<li><a href="/distributor/all_user_stats">Usage Statistics</a></li>
			<li><a href="/distributor/ha-tracker">HA Tracking Status</a></li>
			<li><a href="/multitenant-alertmanager/status">Alertmanager Status</a></li>
			<li><a href="/ring">Ring Status</a></li>
			<li><a href="/ruler/ring">Ruler Ring Status</a></li>
			<li><a href="/services">Service Status</a></li>
			<li><a href="/store-gateway/ring">Ruler Ring Status</a></li>
		</ul>

		<p>Dangerous:</p>
		<ul>
			<li><a href="/flush">Trigger a Flush</a></li>
			<li><a href="/shutdown">Trigger Ingester Shutdown</a></li>
		</ul>
	</body>
</html>`

func indexHandler(w http.ResponseWriter, _ *http.Request) {
	if _, err := w.Write([]byte(indexPageContent)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
