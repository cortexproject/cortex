package telemetry

import rego.v1

# All Cortex distributor metrics must start with cortex_distributor_ or cortex_labels_.
deny contains msg if {
    some group in input.groups
    group.type == "metric"
    not startswith(group.metric_name, "cortex_distributor_")
    not startswith(group.metric_name, "cortex_labels_")
    msg := sprintf("Metric %s does not follow Cortex naming convention (must start with cortex_distributor_ or cortex_labels_)", [group.metric_name])
}

# All metrics must have a brief description.
deny contains msg if {
    some group in input.groups
    group.type == "metric"
    not group.brief
    msg := sprintf("Metric %s is missing a brief description", [group.metric_name])
}
