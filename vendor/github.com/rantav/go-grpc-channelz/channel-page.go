package channelz

import (
	"context"
	"fmt"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

// WriteChannelPage writes an HTML document to w containing per-channel RPC stats, including a header and a footer.
func (h *grpcChannelzHandler) WriteChannelPage(w io.Writer, channel int64) {
	writeHeader(w, fmt.Sprintf("ChannelZ channel %d", channel))
	h.writeChannel(w, channel)
	writeFooter(w)
}

func (h *grpcChannelzHandler) writeChannel(w io.Writer, channel int64) {
	if err := channelTemplate.Execute(w, h.getChannel(channel)); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getChannel(channelID int64) *channelzgrpc.GetChannelResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	channel, err := client.GetChannel(ctx, &channelzgrpc.GetChannelRequest{ChannelId: channelID})
	if err != nil {
		log.Errorf("Error querying GetChannel %+v", err)
		return nil
	}
	return channel
}

const channelTemplateHTML = `
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
    <tr>
		<th>ChannelId</th>
        <td>{{.Channel.Ref.ChannelId}}
	</tr>
    <tr>
		<th>Channel Name</th>
        <td>{{.Channel.Ref.Name}}</td>
	</tr>
	<tr>
        <th>State</th>
        <td>{{.Channel.Data.State}}</td>
	</tr>
	<tr>
        <th>Target</th>
        <td>{{.Channel.Data.Target}}</td>
	</tr>
	<tr>
        <th>Subchannels</th>
		<td>
			{{range .Channel.SubchannelRef}}
				<a href="{{link "subchannel" .SubchannelId}}"><b>{{.SubchannelId}}</b> {{.Name}}</a><br/>
			{{end}}
		</td>
	</tr>
	<tr>
        <th>Child Channels</th>
		<td>
			{{range .Channel.ChannelRef}}
				<a href="{{link "channel" .ChannelId}}"><b>{{.ChannelId}}</b> {{.Name}}</a><br/>
			{{end}}
		</td>
	</tr>
	<tr>
        <th>Sockets</th>
		<td>
			{{range .Channel.SocketRef}}
				<a href="{{link "socket" .SocketId}}"><b>{{.SocketId}}</b> {{.Name}}</a><br/>
			{{end}}
		</td>
	</tr>
	<tr>
        <th>CreationTimestamp</th>
        <td>{{.Channel.Data.Trace.CreationTimestamp | timestamp}}</td>
	</tr>
	<tr>
        <th>CallsStarted</th>
        <td>{{.Channel.Data.CallsStarted}}</td>
	</tr>
	<tr>
        <th>CallsSucceeded</th>
        <td>{{.Channel.Data.CallsSucceeded}}</td>
	</tr>
	<tr>
        <th>CallsFailed</th>
        <td>{{.Channel.Data.CallsFailed}}</td>
	</tr>
	<tr>
        <th>LastCallStartedTimestamp</th>
        <td>{{.Channel.Data.LastCallStartedTimestamp | timestamp}}</td>
    </tr>
    <tr>
        <th>Events</th>
        <td>
			<pre>
			{{- range .Channel.Data.Trace.Events}}
{{.Severity}} [{{.Timestamp | timestamp}}]: {{.Description}}
			{{- end -}}
			</pre>
		</td>
    </tr>
</table>
`
