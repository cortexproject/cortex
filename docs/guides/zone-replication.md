---
title: "Zone Aware Replication"
linkTitle: "Zone Aware Replication"
weight: 5
slug: zone-aware-replication
---

In a default configuration, time-series written to ingesters are replicated based on the container/pod name of the ingester instances. It is completely possible that all the replicas for the given time-series are held with in the same availability zone, even if the cortex infrastructure spans multiple zones within the region. Storing multiple replicas for a given time-series poses a risk for data loss if there is an outage affecting various nodes within a zone or a total outage.

## Configuration

Cortex can be configured to consider an availability zone value in its replication system. Doing so mitigates risks associated with losing multiple nodes within the same availability zone. The availability zone for an ingester can be defined on the command line of the ingester using the `ingester.availability-zone` flag or using the yaml configuration:

```yaml
ingester:
      lifecycler:
        availability_zone: "zone-3"
```

## Zone Replication Considerations

Enabling availability zone awareness helps mitigate risks regarding data loss within a single zone, some items need consideration by an operator if they are thinking of enabling this feature.

### Minimum number of Zones

For cortex to function correctly, there must be at least the same number of availability zones as there is replica count. So by default, a cortex cluster should be spread over 3 zones as the default replica count is 3. It is safe to have more zones than the replica count, but it cannot be less. Having fewer availability zones than replica count causes a replica write to be missed, and in some cases, the write fails if the availability zone count is too low.

### Cost

Depending on the existing cortex infrastructure being used, this may cause an increase in running costs as most cloud providers charge for cross availability zone traffic. The most significant change would be for a cortex cluster currently running in a singular zone.