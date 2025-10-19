package channelz

import (
	"context"
	"fmt"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

func (h *grpcChannelzHandler) WriteServerPage(w io.Writer, server int64) {
	writeHeader(w, fmt.Sprintf("ChannelZ server %d", server))
	h.writeServer(w, server)
	writeFooter(w)
}

// writeServer writes HTML to w containing RPC single server stats.
//
// It includes neither a header nor footer, so you can embed this data in other pages.
func (h *grpcChannelzHandler) writeServer(w io.Writer, server int64) {
	if err := serverTemplate.Execute(w, h.getServer(server)); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getServer(serverID int64) *channelzgrpc.GetServerResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	server, err := client.GetServer(ctx, &channelzgrpc.GetServerRequest{ServerId: serverID})
	if err != nil {
		log.Errorf("Error querying GetServer %+v", err)
		return nil
	}
	return server
}

const serverTemplateHTML = `
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
    <tr>
		<th>ServerId</th>
        <td>{{.Server.Ref.ServerId}}</td>
	</tr>
    <tr>
		<th>Server Name</th>
        <td>{{.Server.Ref.Name}}</td>
	</tr>
	<tr>
		<th>CreationTimestamp</th>
        <td>{{with .Server.Data.Trace}} {{.CreationTimestamp | timestamp}} {{end}}</td>
	</tr>
	<tr>
        <th>CallsStarted</th>
        <td>{{.Server.Data.CallsStarted}}</td>
	</tr>
	<tr>
        <th>CallsSucceeded</th>
        <td>{{.Server.Data.CallsSucceeded}}</td>
	</tr>
	<tr>
        <th>CallsFailed</th>
        <td>{{.Server.Data.CallsFailed}}</td>
	</tr>
	<tr>
        <th>LastCallStartedTimestamp</th>
        <td>{{.Server.Data.LastCallStartedTimestamp | timestamp}}</td>
	</tr>
	<tr>
		<th>Sockets</th>
		<td>
			{{range .Server.ListenSocket}}
				<a href="{{link "socket" .SocketId}}"><b>{{.SocketId}}</b> {{.Name}}</a> <br/>
			{{end}}
		</td>
    </tr>
	{{with .Server.Data.Trace}}
		<tr>
			<th>Events</th>
			<td>
				<pre>
				{{- range .Events}}
{{.Severity}} [{{.Timestamp | timestamp}}]: {{.Description}}
				{{- end -}}
				</pre>
			</td>
		</tr>
	{{end}}
</table>
`
