package channelz

import (
	"io"
	"text/template"
	"time"

	log "google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	common             *template.Template
	headerTemplate     = parseTemplate("header", headerTemplateHTML)
	channelsTemplate   = parseTemplate("channels", channelsTemplateHTML)
	subChannelTemplate = parseTemplate("subchannel", subChannelsTemplateHTML)
	channelTemplate    = parseTemplate("channel", channelTemplateHTML)
	serversTemplate    = parseTemplate("servers", serversTemplateHTML)
	serverTemplate     = parseTemplate("server", serverTemplateHTML)
	socketTemplate     = parseTemplate("socket", socketTemplateHTML)
	footerTemplate     = parseTemplate("footer", footerTemplateHTML)
)

func parseTemplate(name, html string) *template.Template {
	if common == nil {
		common = template.Must(template.New(name).Funcs(getFuncs()).Parse(html))
		return common
	}
	common = template.Must(common.New(name).Funcs(getFuncs()).Parse(html))
	return common
}

func getFuncs() template.FuncMap {
	return template.FuncMap{
		"timestamp": formatTimestamp,
		"link":      createHyperlink,
	}
}

func formatTimestamp(ts *timestamppb.Timestamp) string {
	return ts.AsTime().Format(time.RFC3339)
}

func writeHeader(w io.Writer, title string) {
	if err := headerTemplate.Execute(w, headerData{Title: title}); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

func writeFooter(w io.Writer) {
	if err := footerTemplate.Execute(w, nil); err != nil {
		log.Errorf("channelz: executing template: %v", err)
	}
}

// headerData contains data for the header template.
type headerData struct {
	Title string
}

var (
	headerTemplateHTML = `
<!DOCTYPE html>
<html lang="en"><head>
    <meta charset="utf-8">
    <title>{{.Title}}</title>
    <link rel="stylesheet" href="https://fonts.googleapis.com/icon?family=Material+Icons">
    <link rel="stylesheet" href="https://code.getmdl.io/1.3.0/material.indigo-pink.min.css">
	<style>
		body {padding: 1em}
		table {
			background-color: #fff5ee;
		}
		table.section-header {
			background-color: #eeeeff;
			font-size: x-large;
		}
		table.vertical th {
			text-align: right;
			padding-right: 1em;
		}
		tr.header {
			background-color: #eee5de;
		}
		td {
			vertical-align: top;
		}
		footer {
			padding-top: 1em;
		}
	</style>
</head>
<body>
<h1>{{.Title}}</h1>
`

	footerTemplateHTML = `
<footer>
	<a href="https://github.com/grpc/proposal/blob/master/A14-channelz.md" target="spec">Channelz Spec</a>
</footer>
</body>
</html>
`
)
