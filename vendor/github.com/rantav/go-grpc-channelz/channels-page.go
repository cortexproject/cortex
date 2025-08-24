package channelz

import (
	"context"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

// WriteChannelsPage writes an HTML document to w containing per-channel RPC stats, including a header and a footer.
func (h *grpcChannelzHandler) WriteChannelsPage(w io.Writer, start int64) {
	writeHeader(w, "Channels")
	h.writeChannels(w, start)
	writeFooter(w)
}

// writeTopChannels writes HTML to w containing per-channel RPC stats.
//
// It includes neither a header nor footer, so you can embed this data in other pages.
func (h *grpcChannelzHandler) writeChannels(w io.Writer, start int64) {
	if err := channelsTemplate.Execute(w, h.getTopChannels(start)); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getTopChannels(start int64) *channelzgrpc.GetTopChannelsResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	channels, err := client.GetTopChannels(ctx, &channelzgrpc.GetTopChannelsRequest{
		StartChannelId: start,
	})
	if err != nil {
		log.Errorf("Error querying GetTopChannels %+v", err)
		return nil
	}
	return channels
}

const channelsTemplateHTML = `
{{define "channel-header"}}
    <tr classs="header">
        <th>Channel</th>
        <th>State</th>
        <th>Target</th>
        <th>Subchannels</th>
        <th>Child Channels</th>
        <th>Sockets</th>
        <th>CreationTimestamp</th>
        <th>CallsStarted</th>
        <th>CallsSucceeded</th>
        <th>CallsFailed</th>
        <th>LastCallStartedTimestamp</th>
    </tr>
{{end}}

{{define "channel-body"}}
    <tr>
        <td><a href="{{link "channel" .Ref.ChannelId}}"><b>{{.Ref.ChannelId}}</b> {{.Ref.Name}}</td>
        <td>{{.Data.State}}</td>
        <td>{{.Data.Target}}</td>
		<td>
			{{range .SubchannelRef}}
				<a href="{{link "subchannel" .SubchannelId}}"><b>{{.SubchannelId}}</b> {{.Name}}</a><br/>
			{{end}}
		</td>
		<td>
			{{range .ChannelRef}}
				<a href="{{link "channel" .ChannelId}}"><b>{{.ChannelId}}</b> {{.Name}}</a><br/>
			{{end}}
		</td>
		<td>
			{{range .SocketRef}}
				<a href="{{link "socket" .SocketId}}"><b>{{.SocketId}}</b> {{.Name}}</a><br/>
			{{end}}
		</td>
        <td>{{.Data.Trace.CreationTimestamp | timestamp}}</td>
        <td>{{.Data.CallsStarted}}</td>
        <td>{{.Data.CallsSucceeded}}</td>
        <td>{{.Data.CallsFailed}}</td>
        <td>{{.Data.LastCallStartedTimestamp | timestamp}}</td>
	</tr>
{{end}}
<p><table class="section-header" width=100%><tr align=center><td>Clients</td></tr></table></p>
<table frame=box cellspacing=0 cellpadding=2>
    <tr class="header">
		<th colspan=100 style="text-align:left">Top Channels: {{.Channel | len}}</th>
    </tr>

	{{template "channel-header"}}
	{{$last := .Channel}}
	{{range .Channel}}
		{{template "channel-body" .}}
		{{$last = .}}
	{{end}}
	{{if not .End}}
		<tr>
			<th colspan=100 style="text-align:left">
				<a href="{{link "channels"}}?start={{$last.Ref.ChannelId}}">Next&nbsp;&gt;</a>
			</th>
		</tr>
	{{end}}
</table>
<br/>
<br/>
`
