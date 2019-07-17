# Simple helper for creating prometheus configuration
local prom = {
    # Scrapes pods matching a label named "app"
    PodJob(name, appName): { 
        job_name: name,
        kubernetes_sd_configs: [{
            role: "pod",
        }],
        relabel_configs: [
            # Only include pods matching the "app" label
            {
                action: "keep",
                regex: appName,
                source_labels: [
                    "__meta_kubernetes_pod_label_app",
                ],
            },
            # Only scrape over the 'http' port
            {
                action: "keep",
                regex: "http",
                source_labels: [
                    "__meta_kubernetes_pod_container_port_name",
                ],
            },
            # LabelMap or include existing pod labels
            {
                action: "labelmap",
                regex: "__meta_kubernetes_pod_label_(.+)",
                replacement: "$1",
            },
            # Include the pod's node name in a label
            {
                action: "replace",
                regex: "(.*)",
                replacement: "$1",
                source_labels: ["__meta_kubernetes_pod_node_name"],
                target_label: "kubernetes_node_name",
            },
            # Report the pod's name as "instance"
            {
                action: "replace",
                regex: "(.*)",
                replacement: "$1",
                source_labels: ["__meta_kubernetes_pod_name"],
                target_label: "instance",
            },
        ],
    },
};


{
    # URL of NGinx Proxy
    # This proxy appends the 'X-Org-ScopeID' header and directs traffic to query-frontend or distributor
    local nginx_uri = 'http://nginx.cortex.svc.cluster.local',

    global: {
        external_labels: {
            cortex_deployment: "demo",
        },
        scrape_interval: "30s",
    },
    remote_read: [
        {
            url: nginx_uri + '/api/prom/',
        },
    ],
    remote_write: [
        {
            url: nginx_uri + '/api/prom/push',
        },
    ],
    scrape_configs: [
        prom.PodJob('cortex/alertmanager', 'alertmanager'),
        prom.PodJob('cortex/configs', 'configs'),
        prom.PodJob('cortex/distributor', 'distributor'),
        prom.PodJob('cortex/ingester', 'ingester'),
        prom.PodJob('cortex/memcached', 'memcached'),
        prom.PodJob('cortex/querier', 'querier'),
        prom.PodJob('cortex/query-frontend', 'query-frontend'),
        prom.PodJob('cortex/ruler', 'ruler'),
        prom.PodJob('cortex/table-manager', 'table-manager'),
    ],
}
