package cortex

import (
	"context"
	"net/http"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/server"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// NewServerService constructs service from Server component.
// servicesToWaitFor is called when server is stopping, and should return all
// services that need to terminate before server actually stops.
// N.B.: this function is NOT Cortex specific, please let's keep it that way.
func NewServerService(serv *server.Server, servicesToWaitFor func() []services.Service) services.Service {
	serverDone := make(chan error, 1)

	runFn := func(ctx context.Context) error {
		go func() {
			defer close(serverDone)
			serverDone <- serv.Run()
		}()

		select {
		case <-ctx.Done():
			return nil
		case err := <-serverDone:
			if err != nil {
				level.Error(util.Logger).Log("msg", "server failed", "err", err)
			}
			return err
		}
	}

	stoppingFn := func(_ error) error {
		// wait until all modules are done, and then shutdown server.
		for _, s := range servicesToWaitFor() {
			_ = s.AwaitTerminated(context.Background())
		}

		// shutdown HTTP and gRPC servers (this also unblocks Run)
		serv.Shutdown()

		// if not closed yet, wait until server stops.
		<-serverDone
		level.Info(util.Logger).Log("msg", "server stopped")
		return nil
	}

	return services.NewBasicService(nil, runFn, stoppingFn)
}

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

func indexHandler(w http.ResponseWriter, _ *http.Request) {
	if _, err := w.Write([]byte(indexPageContent)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func configHandler(cfg *Config) http.HandlerFunc {
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
