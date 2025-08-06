package channelz

import (
	"io"
	"net/http"
	"path"
	"strconv"

	"github.com/go-chi/chi/v5"
	log "google.golang.org/grpc/grpclog"
)

type channelzHandler interface {
	WriteTopChannelsPage(io.Writer)
	WriteChannelsPage(io.Writer, int64)
	WriteChannelPage(io.Writer, int64)
	WriteSubchannelPage(io.Writer, int64)
	WriteServerPage(io.Writer, int64)
	WriteSocketPage(io.Writer, int64)
}

var pathPrefix string

func createRouter(prefix string, handler channelzHandler) *chi.Mux {
	pathPrefix = prefix
	router := chi.NewRouter()
	router.Route(prefix, func(r chi.Router) {
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			handler.WriteTopChannelsPage(w)
		})
		r.Get("/channel/{channel}", func(w http.ResponseWriter, r *http.Request) {
			channelStr := chi.URLParam(r, "channel")
			channel, err := strconv.ParseInt(channelStr, 10, 0)
			if err != nil {
				log.Errorf("channelz: Unable to parse int for channel ID. %s", channelStr)
				return
			}
			handler.WriteChannelPage(w, channel)
		})
		r.Get("/channels", func(w http.ResponseWriter, r *http.Request) {
			startStr := r.URL.Query().Get("start")
			start, err := strconv.ParseInt(startStr, 10, 0)
			if err != nil {
				log.Errorf("channelz: Unable to parse int for start channel ID. %s", startStr)
				return
			}
			handler.WriteChannelsPage(w, start)
		})
		r.Get("/subchannel/{channel}", func(w http.ResponseWriter, r *http.Request) {
			channelStr := chi.URLParam(r, "channel")
			channel, err := strconv.ParseInt(channelStr, 10, 0)
			if err != nil {
				log.Errorf("channelz: Unable to parse int for sub-channel ID. %s", channelStr)
				return
			}
			handler.WriteSubchannelPage(w, channel)
		})
		r.Get("/server/{server}", func(w http.ResponseWriter, r *http.Request) {
			serverStr := chi.URLParam(r, "server")
			server, err := strconv.ParseInt(serverStr, 10, 0)
			if err != nil {
				log.Errorf("channelz: Unable to parse int for server ID. %s", serverStr)
				return
			}
			handler.WriteServerPage(w, server)
		})
		r.Get("/socket/{socket}", func(w http.ResponseWriter, r *http.Request) {
			socketStr := chi.URLParam(r, "socket")
			socket, err := strconv.ParseInt(socketStr, 10, 0)
			if err != nil {
				log.Errorf("channelz: Unable to parse int for socket ID. %s", socketStr)
				return
			}
			handler.WriteSocketPage(w, socket)
		})
	})
	return router
}

func createHyperlink(parts ...interface{}) string {
	asStrings := []string{"/" + pathPrefix}
	for _, p := range parts {
		switch t := p.(type) {
		case string:
			asStrings = append(asStrings, t)
		case int:
			s := strconv.Itoa(t)
			asStrings = append(asStrings, s)
		case int64:
			s := strconv.FormatInt(t, 10)
			asStrings = append(asStrings, s)
		}
	}
	return path.Join(asStrings...)
}
