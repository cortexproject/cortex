package channelz

import (
	"context"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

// writeServers writes HTML to w containing RPC servers stats.
//
// It includes neither a header nor footer, so you can embed this data in other pages.
func (h *grpcChannelzHandler) writeServers(w io.Writer) {
	if err := serversTemplate.Execute(w, h.getServers()); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getServers() *channelzgrpc.GetServersResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	servers, err := client.GetServers(ctx, &channelzgrpc.GetServersRequest{})
	if err != nil {
		log.Errorf("Error querying GetServers %+v", err)
		return nil
	}
	return servers
}

const serversTemplateHTML = `
{{define "server-header"}}
    <tr classs="header">
        <th>Server</th>
		<th>CreationTimestamp</th>
        <th>CallsStarted</th>
        <th>CallsSucceeded</th>
        <th>CallsFailed</th>
        <th>LastCallStartedTimestamp</th>
		<th>Sockets</th>
    </tr>
{{end}}

{{define "server-body"}}
    <tr>
        <td><a href="{{link "server" .Ref.ServerId}}"><b>{{.Ref.ServerId}}</b> {{.Ref.Name}}</a></td>
        <td>{{with .Data.Trace}} {{.CreationTimestamp | timestamp}} {{end}}</td>
        <td>{{.Data.CallsStarted}}</td>
        <td>{{.Data.CallsSucceeded}}</td>
        <td>{{.Data.CallsFailed}}</td>
        <td>{{.Data.LastCallStartedTimestamp | timestamp}}</td>
		<td>
			{{range .ListenSocket}}
				<a href="{{link "socket" .SocketId}}"><b>{{.SocketId}}</b> {{.Name}}</a> <br/>
			{{end}}
		</td>
	</tr>
	{{with .Data.Trace}}
		<tr classs="header">
			<th colspan=100>Events</th>
		</tr>
		<tr>
			<td>&nbsp;</td>
			<td colspan=100>
				<pre>
				{{- range .Events}}
{{.Severity}} [{{.Timestamp | timestamp}}]: {{.Description}}
				{{- end -}}
				</pre>
			</td>
		</tr>
	{{end}}
{{end}}

<p><table class="section-header" width=100%><tr align=center><td>Servers</td></tr></table></p>
<table frame=box cellspacing=0 cellpadding=2>
    <tr class="header">
		<th colspan=100 style="text-align:left">Servers: {{.Server | len}}</th>
    </tr>

	{{template "server-header"}}
	{{range .Server}}
		{{template "server-body" .}}
	{{end}}
</table>
`
