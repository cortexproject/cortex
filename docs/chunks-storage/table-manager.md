---
title: "Table-manager"
linkTitle: "Table-manager"
weight: 3
slug: table-manager
---

**Warning: the chunks storage is deprecated. You're encouraged to use the [blocks storage](../blocks-storage/_index.md).**

The table-manager is the Cortex service responsible for creating the [periodic tables](./schema-config.md) used to store index and chunks, and deleting them once their data time range exceeds the retention period (if retention is enabled).

_For more information about the schema config and periodic tables, please refer to the [Schema config](./schema-config.md) documentation._

## Table creation

The table-manager creates new tables slightly ahead of their start period, in order to make sure that the new table is ready once the current table end period is reached. The `-table-manager.periodic-table.grace-period` config option defines how long before a table should be created.

## Retention

The retention - managed by the table-manager - is **disabled by default**, due to its destructive nature. You can enable the data retention explicitly via `-table-manager.retention-deletes-enabled=true` and setting `-table-manager.retention-period` to a value greater than zero.

The table-manager implements the retention deleting the entire tables whose data exceeded the retention period. This design allows to have fast delete operations, at the cost of having a retention granularity controlled by the table's [`period`](./schema-config.md#schema-config).

Given each table contains data for `period` of time and that the entire table is deleted, the table-manager keeps the last tables alive using this formula:

```
number_of_tables_to_keep = floor(retention_period / table_period) + 1
```

![Table-manager retention](/images/chunks-storage/table-manager-retention.png)
<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

It's important to note that - due to the internal implementation - **the table `period` and the retention period must be multiples of `24h`** in order to get the expected behavior.

_For more information about the table-manager configuration, refer to the [config file reference](../configuration/config-file-reference.md#table_manager_config)._

## Active / inactive tables

A table can be active or inactive.

A table is considered **active** if the current time is within the range:
- Table start period - [`-table-manager.periodic-table.grace-period`](../configuration/config-file-reference.md#table_manager_config)
- Table end period + [`-ingester.max-chunk-age`](../configuration/config-file-reference.md#ingester_config)

![Table-manager active_vs_inactive_tables](/images/chunks-storage/table-manager-active-vs-inactive-tables.png)
<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

### DynamoDB index store

Currently, the difference between an active and inactive table **only applies to the DynamoDB storage** settings: capacity mode (on-demand or provisioned), read/write capacity units and autoscaling.

| DynamoDB            | Active table                            | Inactive table                       |
| ------------------- | --------------------------------------- | ------------------------------------ |
| Capacity mode       | `enable_ondemand_throughput_mode`       | `enable_inactive_throughput_on_demand_mode` |
| Read capacity unit  | `provisioned_read_throughput`           | `inactive_read_throughput`           |
| Write capacity unit | `provisioned_write_throughput`          | `inactive_write_throughput`          |
| Autoscaling         | Enabled (if configured)                 | Always disabled                      |

### DynamoDB Provisioning

When configuring DynamoDB with the table-manager, the default [on-demand provisioning](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html) capacity units for reads are set to 300 and writes are set to 3000. The defaults can be overwritten:

```yaml
table_manager:
  index_tables_provisioning:
    provisioned_write_throughput: 10
    provisioned_read_throughput: 10
  chunk_tables_provisioning:
    provisioned_write_throughput: 10
    provisioned_read_throughput: 10
```
