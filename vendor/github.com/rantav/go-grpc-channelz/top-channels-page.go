package channelz

import (
	"io"
)

// WriteTopChannelsPage writes an HTML document to w containing per-channel RPC stats, including a header and a footer.
func (h *grpcChannelzHandler) WriteTopChannelsPage(w io.Writer) {
	writeHeader(w, "ChannelZ Stats")
	h.writeChannels(w, 0)
	h.writeServers(w)
	writeFooter(w)
}
