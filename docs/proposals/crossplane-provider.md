---
title: "Crossplane Provider for Cortex"
linkTitle: "Crossplane Provider for Cortex"
weight: 1
slug: crossplane-provider-for-cortex
---

- Author: [Alex Frisvold](https://github.com/zanderfriz)
- Date: 10/1/2025
- Status: Proposed

## Overview

This proposal outlines the development of a Crossplane provider for Cortex that enables declarative management of a Cortex tenant's recording rules, alerting rules, and Alertmanager configurations through Kubernetes Custom Resources.

## Current State

Currently, a Cortex tenant's recording rules, alerting rules, and Alertmanager configurations are managed through a variety of methods:

- Manual API calls to configure alerting rules and alertmanager settings
- [cortextool](https://github.com/cortexproject/cortex-tools) CLI that directly interacts with Cortex APIs
- Configuration files deployed via traditional Kubernetes deployments or ConfigMaps

This approach leads to several challenges:

- **Configuration Drift**: Manual changes and inconsistent deployment practices result in environments becoming out of sync
- **Limited Observability**: No standardized way to track the status and health of Cortex configurations
- **Security Concerns**: Authentication credentials are often embedded in scripts or stored insecurely
- **Tenant Isolation**: Difficult to enforce proper isolation and governance controls across multiple tenants
- **No GitOps Integration**: Configurations cannot be easily managed through standard Kubernetes workflows

## Proposal

This proposal introduces a Crossplane provider that enables declarative management of Cortex configurations through Kubernetes Custom Resources. The provider implements three core resources that address the key aspects of Cortex management:

1. **TenantConfig**: Manages connection details and authentication for specific Cortex tenants
2. **RuleGroup**: Declaratively manages Prometheus alerting and recording rules
3. **AlertmanagerConfig**: Manages Alertmanager configuration for handling alert notifications

The provider leverages Crossplane's composition and configuration capabilities to provide:

- Secure credential management through Kubernetes secrets
- Declarative configuration management with drift detection
- Multi-tenant isolation and governance controls
- Integration with GitOps workflows
- Comprehensive observability and status reporting

### Goals

- Enable declarative management of Cortex ruler configurations, alerting rules, and Alertmanager configurations through Kubernetes CRDs
- Provide secure authentication and credential management for multi-tenant Cortex environments
- Support multiple authentication methods including bearer tokens, basic auth, and mTLS
- Implement comprehensive observability and status reporting for all managed resources
- Enable platform teams to provide self-service capabilities to development teams while maintaining governance controls
- Support GitOps workflows for configuration management and deployment

### Non-Goals

- This provider does NOT manage the deployment or lifecycle of Cortex infrastructure itself (clusters, storage, etc.)
- Does not provide backup and restore capabilities for Cortex data
- Does not implement custom Cortex extensions or plugins
- Scope is limited to configuration management, not performance tuning or capacity planning

## Design

### Architecture Overview

The provider follows Crossplane's standard architecture pattern with three main components:

```
┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│   TenantConfig  │    │   RuleGroup     │    │AlertmanagerConfig│
│                 │    │                 │    │                  │
│ - Connection    │    │ - Rules Mgmt    │    │ - AM Config      │
│ - Auth Details  │    │ - PromQL        │    │ - Templates      │
│ - TLS Config    │    │ - Evaluation    │    │ - Routing        │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬────────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────────────┐
                    │ Crossplane Provider │
                    │                     │
                    │ - Authentication    │
                    │ - HTTP Client       │
                    │ - Reconciliation    │
                    │ - Status Reporting  │
                    └─────────┬───────────┘
                              │
                    ┌─────────────────────┐
                    │   Cortex Cluster    │
                    │                     │
                    │ - Ruler API         │
                    │ - Alertmanager API  │
                    │ - Multi-tenant      │
                    └─────────────────────┘
```

The provider implements controllers for each Custom Resource Definition (CRD) that handle:

1. **Authentication Management**: Secure retrieval and caching of credentials from Kubernetes secrets
2. **HTTP Client Management**: Configurable HTTP clients with TLS support and custom headers based on Cortex's own go client code
3. **Reconciliation Logic**: Ensures desired state matches actual state in Cortex
4. **Status Reporting**: Provides detailed feedback on resource health and synchronization status
5. **Drift Detection**: Monitors for changes and automatically reconciles configurations

### Custom Resource Definitions (CRDs)

#### TenantConfig

The TenantConfig CRD manages connection details and authentication for a specific Cortex tenant:

```yaml
apiVersion: config.cortexmetrics.io/v1alpha1
kind: TenantConfig
metadata:
  name: production-tenant
  namespace: tenant
spec:
  forProvider:
    tenantId: "production"
    endpoint: "https://cortex.company.com"
    authMethod: "bearer"  # bearer, basic, or none
    bearerTokenSecretRef:
      name: cortex-token
      key: token
    tlsConfig:
      insecureSkipVerify: false
      caBundleSecretRef:
        name: cortex-ca-bundle
        key: ca-bundle.crt
    additionalHeaders:
      "X-Custom-Header": "custom-value"
  providerConfigRef:
    name: cortex-config
status:
  atProvider:
    tenantId: "production"
    connectionStatus: "Connected"
    lastConnectionTime: "2025-01-01T10:30:00Z"
    authMethod: "bearer"
  conditions:
  - type: Ready
    status: "True"
  - type: Synced
    status: "True"
```

**Key Features:**
- Support for multiple authentication methods (bearer token, basic auth, mTLS)
- Flexible TLS configuration including custom CA bundles and client certificates
- Custom HTTP headers for additional authentication or routing
- Connection health monitoring and status reporting

#### RuleGroup

The RuleGroup CRD manages Prometheus alerting and recording rules within a Cortex namespace:

```yaml
apiVersion: config.cortexmetrics.io/v1alpha1
kind: RuleGroup
metadata:
  name: cpu-monitoring
  namespace: tenant
spec:
  forProvider:
    tenantConfigRef:
      name: production-tenant
    namespace: "monitoring"
    groupName: "cpu-alerts"
    interval: "30s"
    rules:
    # Alerting rule - fires when node CPU usage exceeds 80%
    - alert: HighCPUUsage
      expr: |
        100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100) > 80
      for: "5m"
      labels:
        severity: "warning"
        team: "platform"
      annotations:
        summary: "High CPU usage on {{ $labels.instance }}"
        description: "CPU usage is {{ $value | humanize }}% (above 80% threshold for 5 minutes)"
    # Recording rule - pre-calculates node CPU usage percentage for dashboards
    - record: instance_cpu_usage_percent
      expr: |
        100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)
      labels:
        job: "node-exporter"
status:
  atProvider:
    namespace: "monitoring"
    groupName: "cpu-alerts"
    ruleCount: 2
    lastUpdated: "2025-01-01T10:35:00Z"
    status: "Active"
```

**Key Features:**
- Support for both alerting rules (using `alert:` field) and recording rules (using `record:` field)
- Rules specify their type explicitly using either `alert:` or `record:` field
- PromQL expression validation
- Configurable evaluation intervals
- Flexible labeling and annotation support
- Alerting rules support `annotations` and `for` duration fields
- Rule count and status monitoring

#### AlertmanagerConfig

The AlertmanagerConfig CRD manages Alertmanager configuration including routing rules, receivers, and notification templates:

```yaml
apiVersion: config.cortexmetrics.io/v1alpha1
kind: AlertmanagerConfig
metadata:
  name: production-alerting
  namespace: tenant
spec:
  forProvider:
    tenantConfigRef:
      name: production-tenant
    alertmanagerConfig: |
      global:
        resolve_timeout: 5m
        smtp_smarthost: 'smtp.company.com:587'
      route:
        group_by: ['alertname', 'cluster']
        group_wait: 10s
        group_interval: 10s
        repeat_interval: 1h
        receiver: 'platform-team'
        routes:
        - match:
            severity: critical
          receiver: 'critical-alerts'
      receivers:
      - name: 'platform-team'
        email_configs:
        - to: 'platform@company.com'
          subject: 'Alert: {{ .GroupLabels.alertname }}'
          body: '{{ template "alert.html" . }}'
      - name: 'critical-alerts'
        slack_configs:
        - api_url: 'https://hooks.slack.com/services/...'
          channel: '#critical-alerts'
          title: 'Critical Alert: {{ .GroupLabels.alertname }}'
    templateFiles:
      alert.html: |
        <h2>Alert Details</h2>
        {{ range .Alerts }}
        <p><strong>{{ .Annotations.summary }}</strong></p>
        <p>{{ .Annotations.description }}</p>
        {{ end }}
status:
  atProvider:
    configurationStatus: "Applied"
    lastUpdated: "2025-01-01T10:40:00Z"
    configHash: "abc123def456"
```

**Key Features:**
- Full Alertmanager configuration management
- Support for notification templates
- Configuration validation and syntax checking
- Hash-based change detection
- Template file management

### Provider Components

#### Controller Architecture

The provider implements three controllers following Crossplane's managed resource pattern:

**TenantConfig Controller:**
- Manages HTTP client instances with authentication and TLS configuration
- Validates connection to Cortex endpoints and reports connection status
- Caches authentication tokens and manages credential refresh
- Monitors endpoint health and Cortex version compatibility

**RuleGroup Controller:**
- Validates PromQL expressions before applying to Cortex
- Validates rule structure with clear error messages for configuration mistakes
- Manages rule group lifecycle (create, update, delete) via Cortex Ruler API
- Tracks rule evaluation status and provides detailed error reporting
- Supports alerting rules with `alert` name, `for` duration, and `annotations`
- Supports recording rules with `record` name for pre-computed metrics

**AlertmanagerConfig Controller:**
- Validates Alertmanager configuration syntax using built-in validation
- Manages configuration and template files via Cortex Alertmanager API
- Implements hash-based change detection to minimize API calls
- Provides detailed feedback on configuration parsing and validation errors

#### Resource Management

The provider manages resources through a consistent pattern:

1. **Reconciliation Loop**: Each controller runs a reconciliation loop that:
   - Fetches the current state from Cortex APIs
   - Compares with desired state from Kubernetes CRD
   - Applies necessary changes via HTTP API calls
   - Updates resource status with current state and any errors

2. **External Resource Identification**: Resources are identified using:
   - TenantConfig: Combination of endpoint and tenant ID
   - RuleGroup: Namespace and group name within the tenant
   - AlertmanagerConfig: Tenant-specific Alertmanager configuration

3. **Dependency Management**: Resources reference each other using Crossplane's reference resolution:
   - RuleGroup and AlertmanagerConfig reference TenantConfig for connection details
   - Changes to TenantConfig trigger reconciliation of dependent resources

### Configuration Management

The provider handles Cortex configuration through several mechanisms:

**Authentication and Security:**
- Secure retrieval of credentials from Kubernetes secrets
- Support for bearer tokens, basic authentication, and mTLS
- Cross-namespace secret references supported for centralized credential management
- TLS certificate validation with custom CA bundles
- Request timeout and retry configuration

**API Client Management:**
- HTTP client pooling and connection reuse
- Automatic retry with exponential backoff
- Request/response logging for debugging
- Rate limiting to prevent API overwhelm

**Configuration Validation:**
- PromQL expression syntax validation using Prometheus parser
- Rule structure validation with helpful error messages
- Alertmanager configuration validation using Alertmanager's built-in validator
- Pre-flight validation before applying changes to Cortex
- Detailed error reporting for validation failures

**Hash-Based Drift Detection:**
- Configuration hashes computed for both desired state (CRD) and current state (Cortex)
- Hash comparison enables efficient drift detection without deep object comparisons
- Avoids unnecessary API calls and updates when configurations are already synchronized
- Metrics track both drift detected and unnecessary updates avoided

### Observability

The provider includes comprehensive observability features:

**Metrics:**
- **Resource Metrics**: Track rule group sizes, receiver counts, and configuration complexity
- **Performance Metrics**: Monitor hash calculation duration, Cortex API latency, secret retrieval, and TLS setup times
- **Drift Detection Metrics**: Track configuration drift detected and unnecessary updates avoided via hash matching
- **Error Metrics**: Classify and count API errors by type, operation, and resource
- **Authentication Metrics**: Track authentication failures by method
- All metrics designed with low cardinality for efficient Prometheus operation

**Logging:**
The provider will use a **debug-first logging approach** with structured logging:
- **Info Level (Default)**: Only critical events visible
  - Drift detected (with hashes)
  - Connection test failures
  - Blocking deletion events
  - All errors and validation failures
- **Debug Level** (requires `--debug` flag): Operational details
  - All lifecycle operations (Connect, Observe, Create, Update, Delete)
  - "No drift detected" routine checks
  - Connection test start/success
  - API call details and hash calculations
  - TLS/authentication setup
- **Structured Fields**: All logs include resource context (namespace, name, generation, tenantConfig), Cortex details (tenantID, endpoint, cortexNamespace, groupName), and operation-specific data for filtering

**Status Reporting:**
- Standard Crossplane conditions (Ready, Synced)
- Resource-specific status fields (connection status, rule count, config hash, etc.)
- Last update timestamps and configuration hashes
- Error details with actionable messages

## Compatibility
### Crossplane Versions

- **Minimum**: Crossplane v2.0.0 (uses Crossplane Runtime v2)
- **Recommended**: Crossplane v2.0.2

**Note**: Crossplane Runtime v2 is not backwards compatible with Crossplane v1.x releases. This provider requires Crossplane v2.0.0 or later.

### Cortex Versions

The provider supports multiple Cortex versions through its flexible API client:
- **Minimum**: Cortex 1.17.0+
- **Recommended**: Cortex 1.19.0+

### Ruler Storage Backends

The provider requires Cortex to be configured with a ruler storage backend that supports the full Ruler API. The following backends are supported:

| Backend | Support Status | Notes |
|---------|---------------|-------|
| **S3** | ✅ Fully Supported | Recommended for production use. Includes S3-compatible storage like MinIO |
| **GCS** | ✅ Fully Supported | Google Cloud Storage |
| **Azure** | ✅ Fully Supported | Azure Blob Storage |
| **Swift** | ✅ Fully Supported | OpenStack Swift |
| **Local** | ❌ **NOT SUPPORTED** | Read-only backend with no write API support |

**Important**: Cortex's `ruler_storage.backend: local` is a read-only storage backend that does not support the write operations (SetRuleGroup, DeleteRuleGroup) required by this provider.

### API Fallback Support

The provider implements intelligent API fallback for maximum compatibility:

- **Primary Path**: Uses `GetRuleGroup` API for optimal performance on supported backends
- **Fallback Path**: Automatically falls back to `ListRules` if `GetRuleGroup` returns "unsupported" errors
- **Transparency**: Fallback happens automatically with no user configuration required

This ensures compatibility with different Cortex configurations while maintaining optimal performance where possible.

## Security Considerations

### Authentication and Authorization

- **Secure Credential Storage**: All credentials stored in Kubernetes secrets
- **Principle of Least Privilege**: Provider service account has minimal required permissions
- **Bearer Token Authentication**: Maintains continuous authentication using bearer tokens from secrets
- **Audit Logging**: All configuration changes are logged for security auditing

### Network Security

- **TLS**: All HTTP communications can use TLS with certificate validation
- **Custom CA Support**: Support for private CAs and certificate pinning
- **mTLS Authentication**: Client certificate authentication for enhanced security
- **Network Policies**: Compatible with Kubernetes network policies for traffic isolation

### Multi-tenancy

- **Tenant Isolation**: Each TenantConfig manages separate tenant credentials and configurations
- **Namespace Isolation**: Resources can be deployed in separate namespaces for isolation
- **RBAC Integration**: Leverages Kubernetes RBAC for access control
- **Configuration Validation**: Prevents cross-tenant configuration leakage

## Alternatives Considered

### Alternative 1: Helm Charts Only

**Approach**: Using Helm charts to deploy and manage Cortex configurations

**Limitations**:
- No drift detection or reconciliation capabilities
- Manual lifecycle management of configurations
- Limited observability into configuration status
- No native Kubernetes resource management integration
- Difficult to implement proper multi-tenancy and isolation
- Configuration updates require manual intervention

**Conclusion**: Helm charts alone are insufficient for dynamic configuration management and lack the operational benefits of Kubernetes-native resources.

### Alternative 2: Traditional Kubernetes Operators

**Approach**: Building a traditional Kubernetes operator without Crossplane

**Comparison**:
- **Pros**: Direct control over implementation, no external dependencies
- **Cons**:
  - Requires building and maintaining complex controller infrastructure
  - No composition or configuration management capabilities
  - Limited reusability across different Kubernetes clusters
  - Missing advanced features like external secret management
  - Significant development and maintenance overhead

**Conclusion**: While operators provide control, Crossplane offers a mature platform with proven patterns and extensive ecosystem benefits.

### Alternative 3: Terraform Providers

**Approach**: Using Terraform with custom providers for Cortex management

**Comparison**:
- **State Management**: Terraform state vs. Kubernetes-native status
- **Drift Detection**: Limited compared to Kubernetes controllers
- **Integration**: Separate toolchain vs. native Kubernetes workflows
- **Observability**: External monitoring vs. built-in Kubernetes observability
- **Multi-tenancy**: Complex state management vs. native namespace isolation

**Conclusion**: While Terraform is powerful for infrastructure management, Crossplane provides better integration with Kubernetes-native workflows and superior multi-tenancy support for application-level configuration management.

## Open Questions

### Implementation and Technical Questions

- **Configuration Composition**: How should we implement Crossplane Compositions to enable platform teams to create reusable configuration templates?
- **Multi-cluster Support**: What is the best approach for managing Cortex configurations across multiple Kubernetes clusters?
- **Performance Optimization**: What are the optimal reconciliation intervals and caching strategies for large-scale deployments?

### Operational and Deployment Questions

- **Provider Distribution**: What is the best approach for distributing and versioning the provider package?
- **Upgrade Strategy**: How should we handle provider upgrades and backward compatibility for existing configurations?
- **Documentation Strategy**: What additional documentation and training materials are needed for successful adoption?

## References

- [Crossplane Documentation](https://crossplane.io/docs/) - Core platform documentation and best practices
- [Crossplane Runtime v2](https://github.com/crossplane/crossplane-runtime/tree/main/v2) - Runtime library used by the provider
- [Cortex Documentation](https://cortexmetrics.io/docs/) - Cortex configuration and API documentation
- [Cortex Ruler API](https://cortexmetrics.io/docs/api/#ruler) - API endpoints for rule management
- [Cortex Alertmanager API](https://cortexmetrics.io/docs/api/#alertmanager) - API endpoints for alertmanager configuration
