package channelz

import (
	"context"
	"fmt"
	"io"

	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	log "google.golang.org/grpc/grpclog"
)

// WriteSubchannelsPage writes an HTML document to w containing per-channel RPC stats, including a header and a footer.
func (h *grpcChannelzHandler) WriteSubchannelPage(w io.Writer, subchannel int64) {
	writeHeader(w, fmt.Sprintf("ChannelZ subchannel %d", subchannel))
	h.writeSubchannel(w, subchannel)
	writeFooter(w)
}

// writeSubchannel writes HTML to w containing sub-channel RPC stats.
//
// It includes neither a header nor footer, so you can embed this data in other pages.
func (h *grpcChannelzHandler) writeSubchannel(w io.Writer, subchannel int64) {
	if err := subChannelTemplate.Execute(w, h.getSubchannel(subchannel)); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func (h *grpcChannelzHandler) getSubchannel(subchannelID int64) *channelzgrpc.GetSubchannelResponse {
	client, err := h.connect()
	if err != nil {
		log.Errorf("Error creating channelz client %+v", err)
		return nil
	}
	ctx := context.Background()
	subchannel, err := client.GetSubchannel(ctx, &channelzgrpc.GetSubchannelRequest{
		SubchannelId: subchannelID,
	})
	if err != nil {
		log.Errorf("Error querying GetSubchannel %+v", err)
		return nil
	}
	return subchannel
}

const subChannelsTemplateHTML = `
<table frame=box cellspacing=0 cellpadding=2 class="vertical">
    <tr>
		<th>Subchannel</th>
        <td>
			<a href="{{link "subchannel" .Subchannel.Ref.SubchannelId}}">
				<b>{{.Subchannel.Ref.SubchannelId}}</b> {{.Subchannel.Ref.Name}}
			</a>
		</td>
	</tr>
	<tr>
        <th>State</th>
        <td>{{.Subchannel.Data.State}}</td>
	</tr>
	<tr>
        <th>Target</th>
        <td>{{.Subchannel.Data.Target}}</td>
	</tr>
	<tr>
        <th>CreationTimestamp</th>
        <td>{{.Subchannel.Data.Trace.CreationTimestamp | timestamp}}</td>
	</tr>
	<tr>
        <th>CallsStarted</th>
        <td>{{.Subchannel.Data.CallsStarted}}</td>
	</tr>
	<tr>
        <th>CallsSucceeded</th>
        <td>{{.Subchannel.Data.CallsSucceeded}}</td>
	</tr>
	<tr>
        <th>CallsFailed</th>
        <td>{{.Subchannel.Data.CallsFailed}}</td>
	</tr>
	<tr>
        <th>LastCallStartedTimestamp</th>
        <td>{{.Subchannel.Data.LastCallStartedTimestamp | timestamp}}</td>
	</tr>
	<tr>
        <th>Child Channels</th>
		<td>
			{{range .Subchannel.ChannelRef}}
				<b><a href="{{link "channel" .ChannelId}}">{{.ChannelId}}</b> {{.Name}}</a><br/>
			{{end}}
		</td>
	</tr>
	<tr>
        <th>Child Subchannels</th>
		<td>
			{{range .Subchannel.SubchannelRef}}
				<b><a href="{{link "subchannel" .SubchannelId}}">{{.SubchannelId}}</b> {{.Name}}</a><br/>
			{{end}}
		</td>
	</tr>
	<tr>
        <th>Socket</th>
		<td>
			{{range .Subchannel.SocketRef}}
				<b><a href="{{link "socket" .SocketId}}">{{.SocketId}}</b> {{.Name}}</a><br/>
			{{end}}
		</td>
    </tr>
	<tr>
        <th>Events</th>
        <td colspan=100>
			<pre>
			{{- range .Subchannel.Data.Trace.Events}}
{{.Severity}} [{{.Timestamp | timestamp}}]: {{.Description}}
			{{- end -}}
			</pre>
		</td>
    </tr>
</table>
`
