local grafana = import "grafonnet/grafana.libsonnet";

{
    dashboard(title, panelWidth=8, panelHeight=7):: (
        grafana.dashboard.new(
            title,
            time_from="now-1h",
            refresh="1m",
            editable=false,
        ) + {
            local _dashboard = self,
            id:: null,
            _nextPanelPos:: { x: 0, y: 0, prevHeight: 0 },
            addPanel(panel, gridPos={}, height=panelHeight, width=panelWidth):: (
                if std.objectHas(gridPos, 'x') then
                    self + self.addPanels([panel { gridPos: gridPos }]) + {
                        _nextPanelPos: {
                            x: gridPos.x + gridPos.w,
                            y: gridPos.y,
                            prevHeight: gridPos.h,
                        },
                    }

                else
                    local _height = (
                        if panel.type == 'row' then
                            1
                        else
                            height
                    );
                    local _width = (
                        if panel.type == 'row' then
                            24
                        else
                            width
                    );
                    local thisPanelPos = {
                        h: _height,
                        w: _width,
                    } + if _dashboard._nextPanelPos.x + _width > 24 then
                        { x: 0, y: _dashboard._nextPanelPos.y + _dashboard._nextPanelPos.prevHeight }
                    else
                        { x: _dashboard._nextPanelPos.x, y: _dashboard._nextPanelPos.y };

                    self + self.addPanels([panel { gridPos: thisPanelPos }]) + {
                        _nextPanelPos: {
                            x: thisPanelPos.x + _width,
                            y: thisPanelPos.y,
                            prevHeight: _height,
                        },
                    }
            ),
        }
    ),

    row(title, gridPos=null):: {
        collapsed: false,
        gridPos: gridPos,
        panels: [],
        repeat: null,
        title: title,
        type: "row",
    },

    graphPanel(title, targets=[], yFormat='short', yLabel=null, type='default'):: (
        grafana.graphPanel.new(
            title,
            nullPointMode='null as zero',
            format=yFormat,
        ) + {
            targets: [
                grafana.prometheus.target(
                    target.query,
                    datasource='default',
                    legendFormat=target.legendFormat,
                ) + {
                    step: 30,
                    [if std.objectHas(target, 'metric') then 'metric']: target.metric,
                }
                for target in targets
            ],
            yaxes: $.yaxes(yFormat, label=yLabel),
            [if type == 'stack' then 'stack']: true,
            [if type == 'stack' then 'fill']: 10,
            [if type == 'stack' then 'linewidth']: 0,
        }

    ),

    yaxes(format, label=null):: [
        {
            format: format,
            label: label,
            logBase: 1,
            max: null,
            min: null,
            show: true,
        },
        {
            format: "short",
            label: null,
            logBase: 1,
            max: null,
            min: null,
            show: true,
        },
    ],

}
