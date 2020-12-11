package memberlist

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/hashicorp/memberlist"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// This service initialized memberlist.KV on first call to GetMemberlistKV, and starts it. On stop,
// KV is stopped too. If KV fails, error is reported from the service.
type KVInitService struct {
	services.Service

	// config used for initialization
	cfg    *KVConfig
	logger log.Logger

	// init function, to avoid multiple initializations.
	init sync.Once

	// state
	kv      atomic.Value
	err     error
	watcher *services.FailureWatcher
}

func NewKVInitService(cfg *KVConfig, logger log.Logger) *KVInitService {
	kvinit := &KVInitService{
		cfg:     cfg,
		watcher: services.NewFailureWatcher(),
		logger:  logger,
	}
	kvinit.Service = services.NewBasicService(nil, kvinit.running, kvinit.stopping)
	return kvinit
}

// This method will initialize Memberlist.KV on first call, and add it to service failure watcher.
func (kvs *KVInitService) GetMemberlistKV() (*KV, error) {
	kvs.init.Do(func() {
		kv := NewKV(*kvs.cfg, kvs.logger)
		kvs.watcher.WatchService(kv)
		kvs.err = kv.StartAsync(context.Background())

		kvs.kv.Store(kv)
	})

	return kvs.getKV(), kvs.err
}

// Returns KV if it was initialized, or nil.
func (kvs *KVInitService) getKV() *KV {
	return kvs.kv.Load().(*KV)
}

func (kvs *KVInitService) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-kvs.watcher.Chan():
		// Only happens if KV service was actually initialized in GetMemberlistKV and it fails.
		return err
	}
}

func (kvs *KVInitService) stopping(_ error) error {
	kv := kvs.getKV()
	if kv == nil {
		return nil
	}

	return services.StopAndAwaitTerminated(context.Background(), kv)
}

func (kvs *KVInitService) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	kv := kvs.getKV()
	var ml *memberlist.Memberlist
	var store map[string]valueDesc

	if kv != nil {
		ml = kv.memberlist
		store = kv.storeCopy()
	}

	const (
		downloadParam = "download"
		viewParam     = "view"
		viewFormat    = "format"
	)

	if err := req.ParseForm(); err == nil {
		if req.Form[downloadParam] != nil {
			download(w, store, req.Form[downloadParam][0]) // Use first value, ignore the rest.
			return
		}

		if req.Form[viewParam] != nil {
			format := ""
			if len(req.Form[viewFormat]) > 0 {
				format = req.Form[viewFormat][0]
			}

			view(w, kv, store, req.Form[viewParam][0], format)
			return
		}
	}

	members := ml.Members()
	sort.Slice(members, func(i, j int) bool {
		return members[i].Name < members[j].Name
	})

	util.RenderHTTPResponse(w, pageData{
		Now:           time.Now(),
		Initialized:   kv != nil,
		Memberlist:    ml,
		SortedMembers: members,
		Store:         store,
	}, pageTemplate, req)
}

func view(w http.ResponseWriter, kv *KV, store map[string]valueDesc, key string, format string) {
	if kv == nil || store == nil || store[key].value == nil {
		http.Error(w, "value not found", http.StatusNotFound)
		return
	}

	codec := kv.GetCodec(store[key].codecID)
	if codec == nil {
		http.Error(w, "codec not found", http.StatusNotFound)
		return
	}

	val, err := codec.Decode(store[key].value)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to decode: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(200)
	w.Header().Add("content-type", "text/plain")

	switch format {
	case "json", "json-pretty":
		enc := json.NewEncoder(w)
		if format == "json-pretty" {
			enc.SetIndent("", "    ")
		}

		err = enc.Encode(val)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

	default:
		_, _ = fmt.Fprintf(w, "%#v", val)
	}
}

func download(w http.ResponseWriter, store map[string]valueDesc, key string) {
	if store == nil || store[key].value == nil {
		http.Error(w, "value not found", http.StatusNotFound)
		return
	}

	val := store[key]

	w.Header().Add("content-type", "application/octet-stream")
	// Set content-length so that client knows whether it has received full response or not.
	w.Header().Add("content-length", strconv.Itoa(len(val.value)))
	w.Header().Add("content-disposition", fmt.Sprintf("attachment; filename=%d-%s", val.version, key))
	w.WriteHeader(200)

	// Ignore errors, we cannot do anything about them.
	_, _ = w.Write(val.value)
}

type pageData struct {
	Now           time.Time
	Initialized   bool
	Memberlist    *memberlist.Memberlist
	SortedMembers []*memberlist.Node
	Store         map[string]valueDesc
}

var pageTemplate = template.Must(template.New("webpage").Parse(pageContent))

const pageContent = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Cortex Memberlist Status</title>
	</head>
	<body>
		<h1>Cortex Memberlist Status</h1>
		<p>Current time: {{ .Now }}</p>

		{{ if .Initialized }}
		<ul>
		<li>Health Score: {{ .Memberlist.GetHealthScore }} (lower = better, 0 = healthy)</li>
		<li>Members: {{ .Memberlist.NumMembers }}</li>
		</ul>

		<h2>KV Store</h2>

		<table width="100%" border="1">
			<thead>
				<tr>
					<th>Key</th>
					<th>Value Details</th>
					<th>Actions</th>
				</tr>
			</thead>

			<tbody>
				{{ range $k, $v := .Store }}
				<tr>
					<td>{{ $k }}</td>
					<td>{{ $v }}</td>
					<td>
						<a href="?view={{ $k }}&format=json">json</a>
						| <a href="?view={{ $k }}&format=json-pretty">json-pretty</a>
						| <a href="?view={{ $k }}&format=struct">struct</a>
						| <a href="?download={{ $k }}">download</a>
					</td>
				</tr>
				{{ end }}
			</tbody>
		</table>

		<p>Note that value "version" is node-specific. It starts with 0 (on restart), and increases on each received update. Size is in bytes.</p> 

		<h2>Memberlist Cluster Members</h2>

		<table width="100%" border="1">
			<thead>
				<tr>
					<th>Name</th>
					<th>Address</th>
					<th>State</th>
				</tr>
			</thead>

			<tbody>
				{{ range .SortedMembers }}
				<tr>
					<td>{{ .Name }}</td>
					<td>{{ .Address }}</td>
					<td>{{ .State }}</td>
				</tr>
				{{ end }}
			</tbody>
		</table>

		<p>State: 0 = Alive, 1 = Suspect, 2 = Dead, 3 = Left</p>

		{{ else }}
		<p>This Cortex instance doesn't use memberlist.</p>
		{{ end }}
	</body>
</html>`
