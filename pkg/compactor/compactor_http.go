package compactor

import (
	"net/http"

	"github.com/go-kit/kit/log/level"
)

const (
	shardingDisabledPage = `
	<!DOCTYPE html>
	<html>
		<head>
			<meta charset="UTF-8">
			<title>Cortex Compactor Ring</title>
		</head>
		<body>
			<h1>Cortex Compactor Ring</h1>
			<p>Compactor has no ring because sharding is disabled.</p>
		</body>
	</html>
	`
)

func (c *Compactor) RingHandler(w http.ResponseWriter, req *http.Request) {
	if c.compactorCfg.ShardingEnabled {
		c.ring.ServeHTTP(w, req)
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(shardingDisabledPage)); err != nil {
		level.Error(c.logger).Log("msg", "unable to serve compactor ring page", "err", err)
	}
}
