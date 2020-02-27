package cortex

import (
	"net/http"
)

const pageContent = `
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
			<li><a href="/ring">Ring Status</a></li>
			<li><a href="/config">Current Config</a></li>
			<li><a href="/all_user_stats">Usage Statistics</a></li>
			<li><a href="/ha-tracker">HA Tracking Status</a></li>
		</ul>

		<p>Dangerous:</p>
		<ul>
			<li><a href="/flush">Trigger a Flush</a></li>
			<li><a href="/shutdown">Trigger Ingester Shutdown</a></li>
		</ul>
	</body>
</html>`

func (t *Cortex) index(w http.ResponseWriter, r *http.Request) {
	if _, err := w.Write([]byte(pageContent)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
