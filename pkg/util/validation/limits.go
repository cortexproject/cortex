package validation

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/segmentio/fasthash/fnv1a"
	"golang.org/x/time/rate"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

var errMaxGlobalSeriesPerUserValidation = errors.New("The ingester.max-global-series-per-user limit is unsupported if distributor.shard-by-all-labels is disabled")
var errDuplicateQueryPriorities = errors.New("duplicate entry of priorities found. Make sure they are all unique, including the default priority")
var errCompilingQueryPriorityRegex = errors.New("error compiling query priority regex")
var errDuplicatePerLabelSetLimit = errors.New("duplicate per labelSet limits found. Make sure they are all unique")
var errInvalidLabelName = errors.New("invalid label name")
var errInvalidLabelValue = errors.New("invalid label value")

// Supported values for enum limits
const (
	LocalIngestionRateStrategy  = "local"
	GlobalIngestionRateStrategy = "global"
)

// AccessDeniedError are errors that do not comply with the limits specified.
type AccessDeniedError string

func (e AccessDeniedError) Error() string {
	return string(e)
}

// LimitError are errors that do not comply with the limits specified.
type LimitError string

func (e LimitError) Error() string {
	return string(e)
}

type DisabledRuleGroup struct {
	Namespace string `yaml:"namespace" doc:"nocli|description=namespace in which the rule group belongs"`
	Name      string `yaml:"name" doc:"nocli|description=name of the rule group"`
	User      string `yaml:"-" doc:"nocli"`
}

type DisabledRuleGroups []DisabledRuleGroup

type QueryPriority struct {
	Enabled         bool          `yaml:"enabled" json:"enabled"`
	DefaultPriority int64         `yaml:"default_priority" json:"default_priority"`
	Priorities      []PriorityDef `yaml:"priorities" json:"priorities" doc:"nocli|description=List of priority definitions."`
}

type PriorityDef struct {
	Priority         int64            `yaml:"priority" json:"priority" doc:"nocli|description=Priority level. Must be a unique value.|default=0"`
	ReservedQueriers float64          `yaml:"reserved_queriers" json:"reserved_queriers" doc:"nocli|description=Number of reserved queriers to handle priorities higher or equal to the priority level. Value between 0 and 1 will be used as a percentage.|default=0"`
	QueryAttributes  []QueryAttribute `yaml:"query_attributes" json:"query_attributes" doc:"nocli|description=List of query_attributes to match and assign priority to queries. A query is assigned to this priority if it matches any query_attribute in this list. Each query_attribute has several properties (e.g., regex, time_window, user_agent), and all specified properties must match for a query_attribute to be considered a match. Only the specified properties are checked, and an AND operator is applied to them."`
}

type QueryRejection struct {
	Enabled         bool             `yaml:"enabled" json:"enabled"`
	QueryAttributes []QueryAttribute `yaml:"query_attributes" json:"query_attributes" doc:"nocli|description=List of query_attributes to match and reject queries. A query is rejected if it matches any query_attribute in this list. Each query_attribute has several properties (e.g., regex, time_window, user_agent), and all specified properties must match for a query_attribute to be considered a match. Only the specified properties are checked, and an AND operator is applied to them."`
}

type QueryAttribute struct {
	ApiType                string         `yaml:"api_type" json:"api_type" doc:"nocli|description=API type for the query. Should be one of the query, query_range, series, labels, label_values. If not set, it won't be checked."`
	Regex                  string         `yaml:"regex" json:"regex" doc:"nocli|description=Regex that the query string (or at least one of the matchers in metadata query) should match. If not set, it won't be checked."`
	TimeWindow             TimeWindow     `yaml:"time_window" json:"time_window" doc:"nocli|description=Overall data select time window (including range selectors, modifiers and lookback delta) that the query should be within. If not set, it won't be checked."`
	TimeRangeLimit         TimeRangeLimit `yaml:"time_range_limit" json:"time_range_limit" doc:"nocli|description=Query time range should be within this limit to match. Depending on where it was used, in most of the use-cases, either min or max value will be used. If not set, it won't be checked."`
	QueryStepLimit         QueryStepLimit `yaml:"query_step_limit" json:"query_step_limit" doc:"nocli|description=If query step provided should be within this limit to match. If not set, it won't be checked. This property only applied to range queries and ignored for other types of queries."`
	UserAgentRegex         string         `yaml:"user_agent_regex" json:"user_agent_regex" doc:"nocli|description=Regex that User-Agent header of the request should match. If not set, it won't be checked."`
	DashboardUID           string         `yaml:"dashboard_uid" json:"dashboard_uid" doc:"nocli|description=Grafana includes X-Dashboard-Uid header in query requests. If this field is provided then X-Dashboard-Uid header of request should match this value. If not set, it won't be checked. This property won't be applied to metadata queries."`
	PanelID                string         `yaml:"panel_id" json:"panel_id" doc:"nocli|description=Grafana includes X-Panel-Id header in query requests. If this field is provided then X-Panel-Id header of request should match this value. If not set, it won't be checked. This property won't be applied to metadata queries."`
	CompiledRegex          *regexp.Regexp
	CompiledUserAgentRegex *regexp.Regexp
}

type TimeWindow struct {
	Start model.Duration `yaml:"start" json:"start" doc:"nocli|description=Start of the data select time window (including range selectors, modifiers and lookback delta) that the query should be within. If set to 0, it won't be checked.|default=0"`
	End   model.Duration `yaml:"end" json:"end" doc:"nocli|description=End of the data select time window (including range selectors, modifiers and lookback delta) that the query should be within. If set to 0, it won't be checked.|default=0"`
}

type TimeRangeLimit struct {
	Min model.Duration `yaml:"min" json:"min" doc:"nocli|description=This will be duration (12h, 1d, 15d etc.). Query time range should be above or equal to this value to match. Ex: if this value is 20d, then queries whose range is bigger than or equal to 20d will match. If set to 0, it won't be checked.|default=0"`
	Max model.Duration `yaml:"max" json:"max" doc:"nocli|description=This will be duration (12h, 1d, 15d etc.). Query time range should be below or equal to this value to match. Ex: if this value is 24h, then queries whose range is smaller than or equal to 24h will match.If set to 0, it won't be checked.|default=0"`
}

type QueryStepLimit struct {
	Min model.Duration `yaml:"min" json:"min" doc:"nocli|description=Query step should be above or equal to this value to match. If set to 0, it won't be checked.|default=0"`
	Max model.Duration `yaml:"max" json:"max" doc:"nocli|description=Query step should be below or equal to this value to match. If set to 0, it won't be checked.|default=0"`
}

type LimitsPerLabelSetEntry struct {
	MaxSeries int `yaml:"max_series" json:"max_series" doc:"nocli|description=The maximum number of active series per LabelSet, across the cluster before replication. Setting the value 0 will enable the monitoring (metrics) but would not enforce any limits."`
}

type LimitsPerLabelSet struct {
	Limits   LimitsPerLabelSetEntry `yaml:"limits" json:"limits" doc:"nocli"`
	LabelSet labels.Labels          `yaml:"label_set" json:"label_set" doc:"nocli|description=LabelSet which the limit should be applied. If no labels are provided, it becomes the default partition which matches any series that doesn't match any other explicitly defined label sets.'"`
	Id       string                 `yaml:"-" json:"-" doc:"nocli"`
	Hash     uint64                 `yaml:"-" json:"-" doc:"nocli"`
}

// Limits describe all the limits for users; can be used to describe global default
// limits via flags, or per-user limits via yaml config.
type Limits struct {
	// Distributor enforced limits.
	IngestionRate             float64             `yaml:"ingestion_rate" json:"ingestion_rate"`
	IngestionRateStrategy     string              `yaml:"ingestion_rate_strategy" json:"ingestion_rate_strategy"`
	IngestionBurstSize        int                 `yaml:"ingestion_burst_size" json:"ingestion_burst_size"`
	AcceptHASamples           bool                `yaml:"accept_ha_samples" json:"accept_ha_samples"`
	AcceptMixedHASamples      bool                `yaml:"accept_mixed_ha_samples" json:"accept_mixed_ha_samples"`
	HAClusterLabel            string              `yaml:"ha_cluster_label" json:"ha_cluster_label"`
	HAReplicaLabel            string              `yaml:"ha_replica_label" json:"ha_replica_label"`
	HAMaxClusters             int                 `yaml:"ha_max_clusters" json:"ha_max_clusters"`
	DropLabels                flagext.StringSlice `yaml:"drop_labels" json:"drop_labels"`
	MaxLabelNameLength        int                 `yaml:"max_label_name_length" json:"max_label_name_length"`
	MaxLabelValueLength       int                 `yaml:"max_label_value_length" json:"max_label_value_length"`
	MaxLabelNamesPerSeries    int                 `yaml:"max_label_names_per_series" json:"max_label_names_per_series"`
	MaxLabelsSizeBytes        int                 `yaml:"max_labels_size_bytes" json:"max_labels_size_bytes"`
	MaxMetadataLength         int                 `yaml:"max_metadata_length" json:"max_metadata_length"`
	RejectOldSamples          bool                `yaml:"reject_old_samples" json:"reject_old_samples"`
	RejectOldSamplesMaxAge    model.Duration      `yaml:"reject_old_samples_max_age" json:"reject_old_samples_max_age"`
	CreationGracePeriod       model.Duration      `yaml:"creation_grace_period" json:"creation_grace_period"`
	EnforceMetadataMetricName bool                `yaml:"enforce_metadata_metric_name" json:"enforce_metadata_metric_name"`
	EnforceMetricName         bool                `yaml:"enforce_metric_name" json:"enforce_metric_name"`
	IngestionTenantShardSize  int                 `yaml:"ingestion_tenant_shard_size" json:"ingestion_tenant_shard_size"`
	MetricRelabelConfigs      []*relabel.Config   `yaml:"metric_relabel_configs,omitempty" json:"metric_relabel_configs,omitempty" doc:"nocli|description=List of metric relabel configurations. Note that in most situations, it is more effective to use metrics relabeling directly in the Prometheus server, e.g. remote_write.write_relabel_configs."`
	MaxNativeHistogramBuckets int                 `yaml:"max_native_histogram_buckets" json:"max_native_histogram_buckets"`
	PromoteResourceAttributes []string            `yaml:"promote_resource_attributes" json:"promote_resource_attributes"`

	// Ingester enforced limits.
	// Series
	MaxLocalSeriesPerUser    int                 `yaml:"max_series_per_user" json:"max_series_per_user"`
	MaxLocalSeriesPerMetric  int                 `yaml:"max_series_per_metric" json:"max_series_per_metric"`
	MaxGlobalSeriesPerUser   int                 `yaml:"max_global_series_per_user" json:"max_global_series_per_user"`
	MaxGlobalSeriesPerMetric int                 `yaml:"max_global_series_per_metric" json:"max_global_series_per_metric"`
	LimitsPerLabelSet        []LimitsPerLabelSet `yaml:"limits_per_label_set" json:"limits_per_label_set" doc:"nocli|description=[Experimental] Enable limits per LabelSet. Supported limits per labelSet: [max_series]"`

	// Metadata
	MaxLocalMetricsWithMetadataPerUser  int `yaml:"max_metadata_per_user" json:"max_metadata_per_user"`
	MaxLocalMetadataPerMetric           int `yaml:"max_metadata_per_metric" json:"max_metadata_per_metric"`
	MaxGlobalMetricsWithMetadataPerUser int `yaml:"max_global_metadata_per_user" json:"max_global_metadata_per_user"`
	MaxGlobalMetadataPerMetric          int `yaml:"max_global_metadata_per_metric" json:"max_global_metadata_per_metric"`
	// Out-of-order
	OutOfOrderTimeWindow model.Duration `yaml:"out_of_order_time_window" json:"out_of_order_time_window"`
	// Exemplars
	MaxExemplars int `yaml:"max_exemplars" json:"max_exemplars"`

	// Querier enforced limits.
	MaxChunksPerQuery            int            `yaml:"max_fetched_chunks_per_query" json:"max_fetched_chunks_per_query"`
	MaxFetchedSeriesPerQuery     int            `yaml:"max_fetched_series_per_query" json:"max_fetched_series_per_query"`
	MaxFetchedChunkBytesPerQuery int            `yaml:"max_fetched_chunk_bytes_per_query" json:"max_fetched_chunk_bytes_per_query"`
	MaxFetchedDataBytesPerQuery  int            `yaml:"max_fetched_data_bytes_per_query" json:"max_fetched_data_bytes_per_query"`
	MaxQueryLookback             model.Duration `yaml:"max_query_lookback" json:"max_query_lookback"`
	MaxQueryLength               model.Duration `yaml:"max_query_length" json:"max_query_length"`
	MaxQueryParallelism          int            `yaml:"max_query_parallelism" json:"max_query_parallelism"`
	MaxCacheFreshness            model.Duration `yaml:"max_cache_freshness" json:"max_cache_freshness"`
	MaxQueriersPerTenant         float64        `yaml:"max_queriers_per_tenant" json:"max_queriers_per_tenant"`
	QueryVerticalShardSize       int            `yaml:"query_vertical_shard_size" json:"query_vertical_shard_size" doc:"hidden"`
	QueryPartialData             bool           `yaml:"query_partial_data" json:"query_partial_data" doc:"nocli|description=Enable to allow queries to be evaluated with data from a single zone, if other zones are not available.|default=false"`

	// Query Frontend / Scheduler enforced limits.
	MaxOutstandingPerTenant     int           `yaml:"max_outstanding_requests_per_tenant" json:"max_outstanding_requests_per_tenant"`
	QueryPriority               QueryPriority `yaml:"query_priority" json:"query_priority" doc:"nocli|description=Configuration for query priority."`
	queryAttributeRegexHash     uint64
	queryAttributeCompiledRegex map[string]*regexp.Regexp
	QueryRejection              QueryRejection `yaml:"query_rejection" json:"query_rejection" doc:"nocli|description=Configuration for query rejection."`

	// Ruler defaults and limits.
	RulerEvaluationDelay        model.Duration `yaml:"ruler_evaluation_delay_duration" json:"ruler_evaluation_delay_duration"`
	RulerTenantShardSize        int            `yaml:"ruler_tenant_shard_size" json:"ruler_tenant_shard_size"`
	RulerMaxRulesPerRuleGroup   int            `yaml:"ruler_max_rules_per_rule_group" json:"ruler_max_rules_per_rule_group"`
	RulerMaxRuleGroupsPerTenant int            `yaml:"ruler_max_rule_groups_per_tenant" json:"ruler_max_rule_groups_per_tenant"`
	RulerQueryOffset            model.Duration `yaml:"ruler_query_offset" json:"ruler_query_offset"`
	RulerExternalLabels         labels.Labels  `yaml:"ruler_external_labels" json:"ruler_external_labels" doc:"nocli|description=external labels for alerting rules"`
	RulesPartialData            bool           `yaml:"rules_partial_data" json:"rules_partial_data" doc:"nocli|description=Enable to allow rules to be evaluated with data from a single zone, if other zones are not available.|default=false"`

	// Store-gateway.
	StoreGatewayTenantShardSize  float64 `yaml:"store_gateway_tenant_shard_size" json:"store_gateway_tenant_shard_size"`
	MaxDownloadedBytesPerRequest int     `yaml:"max_downloaded_bytes_per_request" json:"max_downloaded_bytes_per_request"`

	// Compactor.
	CompactorBlocksRetentionPeriod   model.Duration `yaml:"compactor_blocks_retention_period" json:"compactor_blocks_retention_period"`
	CompactorTenantShardSize         int            `yaml:"compactor_tenant_shard_size" json:"compactor_tenant_shard_size"`
	CompactorPartitionIndexSizeBytes int64          `yaml:"compactor_partition_index_size_bytes" json:"compactor_partition_index_size_bytes"`
	CompactorPartitionSeriesCount    int64          `yaml:"compactor_partition_series_count" json:"compactor_partition_series_count"`

	// This config doesn't have a CLI flag registered here because they're registered in
	// their own original config struct.
	S3SSEType                 string `yaml:"s3_sse_type" json:"s3_sse_type" doc:"nocli|description=S3 server-side encryption type. Required to enable server-side encryption overrides for a specific tenant. If not set, the default S3 client settings are used."`
	S3SSEKMSKeyID             string `yaml:"s3_sse_kms_key_id" json:"s3_sse_kms_key_id" doc:"nocli|description=S3 server-side encryption KMS Key ID. Ignored if the SSE type override is not set."`
	S3SSEKMSEncryptionContext string `yaml:"s3_sse_kms_encryption_context" json:"s3_sse_kms_encryption_context" doc:"nocli|description=S3 server-side encryption KMS encryption context. If unset and the key ID override is set, the encryption context will not be provided to S3. Ignored if the SSE type override is not set."`

	// Alertmanager.
	AlertmanagerReceiversBlockCIDRNetworks     flagext.CIDRSliceCSV `yaml:"alertmanager_receivers_firewall_block_cidr_networks" json:"alertmanager_receivers_firewall_block_cidr_networks"`
	AlertmanagerReceiversBlockPrivateAddresses bool                 `yaml:"alertmanager_receivers_firewall_block_private_addresses" json:"alertmanager_receivers_firewall_block_private_addresses"`

	NotificationRateLimit               float64                  `yaml:"alertmanager_notification_rate_limit" json:"alertmanager_notification_rate_limit"`
	NotificationRateLimitPerIntegration NotificationRateLimitMap `yaml:"alertmanager_notification_rate_limit_per_integration" json:"alertmanager_notification_rate_limit_per_integration"`

	AlertmanagerMaxConfigSizeBytes             int                `yaml:"alertmanager_max_config_size_bytes" json:"alertmanager_max_config_size_bytes"`
	AlertmanagerMaxTemplatesCount              int                `yaml:"alertmanager_max_templates_count" json:"alertmanager_max_templates_count"`
	AlertmanagerMaxTemplateSizeBytes           int                `yaml:"alertmanager_max_template_size_bytes" json:"alertmanager_max_template_size_bytes"`
	AlertmanagerMaxDispatcherAggregationGroups int                `yaml:"alertmanager_max_dispatcher_aggregation_groups" json:"alertmanager_max_dispatcher_aggregation_groups"`
	AlertmanagerMaxAlertsCount                 int                `yaml:"alertmanager_max_alerts_count" json:"alertmanager_max_alerts_count"`
	AlertmanagerMaxAlertsSizeBytes             int                `yaml:"alertmanager_max_alerts_size_bytes" json:"alertmanager_max_alerts_size_bytes"`
	AlertmanagerMaxSilencesCount               int                `yaml:"alertmanager_max_silences_count" json:"alertmanager_max_silences_count"`
	AlertmanagerMaxSilencesSizeBytes           int                `yaml:"alertmanager_max_silences_size_bytes" json:"alertmanager_max_silences_size_bytes"`
	DisabledRuleGroups                         DisabledRuleGroups `yaml:"disabled_rule_groups" json:"disabled_rule_groups" doc:"nocli|description=list of rule groups to disable"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (l *Limits) RegisterFlags(f *flag.FlagSet) {
	flagext.DeprecatedFlag(f, "ingester.max-series-per-query", "Deprecated: The maximum number of series for which a query can fetch samples from each ingester. This limit is enforced only in the ingesters (when querying samples not flushed to the storage yet) and it's a per-instance limit. This limit is ignored when running the Cortex blocks storage. When running Cortex with blocks storage use -querier.max-fetched-series-per-query limit instead.", util_log.Logger)

	f.IntVar(&l.IngestionTenantShardSize, "distributor.ingestion-tenant-shard-size", 0, "The default tenant's shard size when the shuffle-sharding strategy is used. Must be set both on ingesters and distributors. When this setting is specified in the per-tenant overrides, a value of 0 disables shuffle sharding for the tenant.")
	f.Float64Var(&l.IngestionRate, "distributor.ingestion-rate-limit", 25000, "Per-user ingestion rate limit in samples per second.")
	f.StringVar(&l.IngestionRateStrategy, "distributor.ingestion-rate-limit-strategy", "local", "Whether the ingestion rate limit should be applied individually to each distributor instance (local), or evenly shared across the cluster (global).")
	f.IntVar(&l.IngestionBurstSize, "distributor.ingestion-burst-size", 50000, "Per-user allowed ingestion burst size (in number of samples).")
	f.BoolVar(&l.AcceptHASamples, "distributor.ha-tracker.enable-for-all-users", false, "Flag to enable, for all users, handling of samples with external labels identifying replicas in an HA Prometheus setup.")
	f.BoolVar(&l.AcceptMixedHASamples, "experimental.distributor.ha-tracker.mixed-ha-samples", false, "[Experimental] Flag to enable handling of samples with mixed external labels identifying replicas in an HA Prometheus setup. Supported only if -distributor.ha-tracker.enable-for-all-users is true.")
	f.StringVar(&l.HAClusterLabel, "distributor.ha-tracker.cluster", "cluster", "Prometheus label to look for in samples to identify a Prometheus HA cluster.")
	f.StringVar(&l.HAReplicaLabel, "distributor.ha-tracker.replica", "__replica__", "Prometheus label to look for in samples to identify a Prometheus HA replica.")
	f.IntVar(&l.HAMaxClusters, "distributor.ha-tracker.max-clusters", 0, "Maximum number of clusters that HA tracker will keep track of for single user. 0 to disable the limit.")
	f.Var((*flagext.StringSliceCSV)(&l.PromoteResourceAttributes), "distributor.promote-resource-attributes", "Comma separated list of resource attributes that should be converted to labels.")
	f.Var(&l.DropLabels, "distributor.drop-label", "This flag can be used to specify label names that to drop during sample ingestion within the distributor and can be repeated in order to drop multiple labels.")
	f.IntVar(&l.MaxLabelNameLength, "validation.max-length-label-name", 1024, "Maximum length accepted for label names")
	f.IntVar(&l.MaxLabelValueLength, "validation.max-length-label-value", 2048, "Maximum length accepted for label value. This setting also applies to the metric name")
	f.IntVar(&l.MaxLabelNamesPerSeries, "validation.max-label-names-per-series", 30, "Maximum number of label names per series.")
	f.IntVar(&l.MaxLabelsSizeBytes, "validation.max-labels-size-bytes", 0, "Maximum combined size in bytes of all labels and label values accepted for a series. 0 to disable the limit.")
	f.IntVar(&l.MaxMetadataLength, "validation.max-metadata-length", 1024, "Maximum length accepted for metric metadata. Metadata refers to Metric Name, HELP and UNIT.")
	f.BoolVar(&l.RejectOldSamples, "validation.reject-old-samples", false, "Reject old samples.")
	_ = l.RejectOldSamplesMaxAge.Set("14d")
	f.Var(&l.RejectOldSamplesMaxAge, "validation.reject-old-samples.max-age", "Maximum accepted sample age before rejecting.")
	_ = l.CreationGracePeriod.Set("10m")
	f.Var(&l.CreationGracePeriod, "validation.create-grace-period", "Duration which table will be created/deleted before/after it's needed; we won't accept sample from before this time.")
	f.BoolVar(&l.EnforceMetricName, "validation.enforce-metric-name", true, "Enforce every sample has a metric name.")
	f.BoolVar(&l.EnforceMetadataMetricName, "validation.enforce-metadata-metric-name", true, "Enforce every metadata has a metric name.")
	f.IntVar(&l.MaxNativeHistogramBuckets, "validation.max-native-histogram-buckets", 0, "Limit on total number of positive and negative buckets allowed in a single native histogram. The resolution of a histogram with more buckets will be reduced until the number of buckets is within the limit. If the limit cannot be reached, the sample will be discarded. 0 means no limit. Enforced at Distributor.")

	f.IntVar(&l.MaxLocalSeriesPerUser, "ingester.max-series-per-user", 5000000, "The maximum number of active series per user, per ingester. 0 to disable.")
	f.IntVar(&l.MaxLocalSeriesPerMetric, "ingester.max-series-per-metric", 50000, "The maximum number of active series per metric name, per ingester. 0 to disable.")
	f.IntVar(&l.MaxGlobalSeriesPerUser, "ingester.max-global-series-per-user", 0, "The maximum number of active series per user, across the cluster before replication. 0 to disable. Supported only if -distributor.shard-by-all-labels is true.")
	f.IntVar(&l.MaxGlobalSeriesPerMetric, "ingester.max-global-series-per-metric", 0, "The maximum number of active series per metric name, across the cluster before replication. 0 to disable.")
	f.IntVar(&l.MaxExemplars, "ingester.max-exemplars", 0, "Enables support for exemplars in TSDB and sets the maximum number that will be stored. less than zero means disabled. If the value is set to zero, cortex will fallback to blocks-storage.tsdb.max-exemplars value.")
	f.Var(&l.OutOfOrderTimeWindow, "ingester.out-of-order-time-window", "[Experimental] Configures the allowed time window for ingestion of out-of-order samples. Disabled (0s) by default.")

	f.IntVar(&l.MaxLocalMetricsWithMetadataPerUser, "ingester.max-metadata-per-user", 8000, "The maximum number of active metrics with metadata per user, per ingester. 0 to disable.")
	f.IntVar(&l.MaxLocalMetadataPerMetric, "ingester.max-metadata-per-metric", 10, "The maximum number of metadata per metric, per ingester. 0 to disable.")
	f.IntVar(&l.MaxGlobalMetricsWithMetadataPerUser, "ingester.max-global-metadata-per-user", 0, "The maximum number of active metrics with metadata per user, across the cluster. 0 to disable. Supported only if -distributor.shard-by-all-labels is true.")
	f.IntVar(&l.MaxGlobalMetadataPerMetric, "ingester.max-global-metadata-per-metric", 0, "The maximum number of metadata per metric, across the cluster. 0 to disable.")
	f.IntVar(&l.MaxChunksPerQuery, "querier.max-fetched-chunks-per-query", 2000000, "Maximum number of chunks that can be fetched in a single query from ingesters and long-term storage. This limit is enforced in the querier, ruler and store-gateway. 0 to disable.")
	f.IntVar(&l.MaxFetchedSeriesPerQuery, "querier.max-fetched-series-per-query", 0, "The maximum number of unique series for which a query can fetch samples from each ingesters and blocks storage. This limit is enforced in the querier, ruler and store-gateway. 0 to disable")
	f.IntVar(&l.MaxFetchedChunkBytesPerQuery, "querier.max-fetched-chunk-bytes-per-query", 0, "Deprecated (use max-fetched-data-bytes-per-query instead): The maximum size of all chunks in bytes that a query can fetch from each ingester and storage. This limit is enforced in the querier, ruler and store-gateway. 0 to disable.")
	f.IntVar(&l.MaxFetchedDataBytesPerQuery, "querier.max-fetched-data-bytes-per-query", 0, "The maximum combined size of all data that a query can fetch from each ingester and storage. This limit is enforced in the querier and ruler for `query`, `query_range` and `series` APIs. 0 to disable.")
	f.Var(&l.MaxQueryLength, "store.max-query-length", "Limit the query time range (end - start time of range query parameter and max - min of data fetched time range). This limit is enforced in the query-frontend and ruler (on the received query). 0 to disable.")
	f.Var(&l.MaxQueryLookback, "querier.max-query-lookback", "Limit how long back data (series and metadata) can be queried, up until <lookback> duration ago. This limit is enforced in the query-frontend, querier and ruler. If the requested time range is outside the allowed range, the request will not fail but will be manipulated to only query data within the allowed time range. 0 to disable.")
	f.IntVar(&l.MaxQueryParallelism, "querier.max-query-parallelism", 14, "Maximum number of split queries will be scheduled in parallel by the frontend.")
	_ = l.MaxCacheFreshness.Set("1m")
	f.Var(&l.MaxCacheFreshness, "frontend.max-cache-freshness", "Most recent allowed cacheable result per-tenant, to prevent caching very recent results that might still be in flux.")
	f.Float64Var(&l.MaxQueriersPerTenant, "frontend.max-queriers-per-tenant", 0, "Maximum number of queriers that can handle requests for a single tenant. If set to 0 or value higher than number of available queriers, *all* queriers will handle requests for the tenant. If the value is < 1, it will be treated as a percentage and the gets a percentage of the total queriers. Each frontend (or query-scheduler, if used) will select the same set of queriers for the same tenant (given that all queriers are connected to all frontends / query-schedulers). This option only works with queriers connecting to the query-frontend / query-scheduler, not when using downstream URL.")
	f.IntVar(&l.QueryVerticalShardSize, "frontend.query-vertical-shard-size", 0, "[Experimental] Number of shards to use when distributing shardable PromQL queries.")
	f.BoolVar(&l.QueryPriority.Enabled, "frontend.query-priority.enabled", false, "Whether queries are assigned with priorities.")
	f.Int64Var(&l.QueryPriority.DefaultPriority, "frontend.query-priority.default-priority", 0, "Priority assigned to all queries by default. Must be a unique value. Use this as a baseline to make certain queries higher/lower priority.")
	f.BoolVar(&l.QueryRejection.Enabled, "frontend.query-rejection.enabled", false, "Whether query rejection is enabled.")

	f.IntVar(&l.MaxOutstandingPerTenant, "frontend.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per request queue (either query frontend or query scheduler); requests beyond this error with HTTP 429.")

	f.Var(&l.RulerEvaluationDelay, "ruler.evaluation-delay-duration", "Deprecated(use ruler.query-offset instead) and will be removed in v1.19.0: Duration to delay the evaluation of rules to ensure the underlying metrics have been pushed to Cortex.")
	f.IntVar(&l.RulerTenantShardSize, "ruler.tenant-shard-size", 0, "The default tenant's shard size when the shuffle-sharding strategy is used by ruler. When this setting is specified in the per-tenant overrides, a value of 0 disables shuffle sharding for the tenant.")
	f.IntVar(&l.RulerMaxRulesPerRuleGroup, "ruler.max-rules-per-rule-group", 0, "Maximum number of rules per rule group per-tenant. 0 to disable.")
	f.IntVar(&l.RulerMaxRuleGroupsPerTenant, "ruler.max-rule-groups-per-tenant", 0, "Maximum number of rule groups per-tenant. 0 to disable.")
	f.Var(&l.RulerQueryOffset, "ruler.query-offset", "Duration to offset all rule evaluation queries per-tenant.")

	f.Var(&l.CompactorBlocksRetentionPeriod, "compactor.blocks-retention-period", "Delete blocks containing samples older than the specified retention period. 0 to disable.")
	f.IntVar(&l.CompactorTenantShardSize, "compactor.tenant-shard-size", 0, "The default tenant's shard size when the shuffle-sharding strategy is used by the compactor. When this setting is specified in the per-tenant overrides, a value of 0 disables shuffle sharding for the tenant.")
	// Default to 64GB because this is the hard limit of index size in Cortex
	f.Int64Var(&l.CompactorPartitionIndexSizeBytes, "compactor.partition-index-size-bytes", 68719476736, "Index size limit in bytes for each compaction partition. 0 means no limit")
	f.Int64Var(&l.CompactorPartitionSeriesCount, "compactor.partition-series-count", 0, "Time series count limit for each compaction partition. 0 means no limit")

	// Store-gateway.
	f.Float64Var(&l.StoreGatewayTenantShardSize, "store-gateway.tenant-shard-size", 0, "The default tenant's shard size when the shuffle-sharding strategy is used. Must be set when the store-gateway sharding is enabled with the shuffle-sharding strategy. When this setting is specified in the per-tenant overrides, a value of 0 disables shuffle sharding for the tenant. If the value is < 1 the shard size will be a percentage of the total store-gateways.")
	f.IntVar(&l.MaxDownloadedBytesPerRequest, "store-gateway.max-downloaded-bytes-per-request", 0, "The maximum number of data bytes to download per gRPC request in Store Gateway, including Series/LabelNames/LabelValues requests. 0 to disable.")

	// Alertmanager.
	f.Var(&l.AlertmanagerReceiversBlockCIDRNetworks, "alertmanager.receivers-firewall-block-cidr-networks", "Comma-separated list of network CIDRs to block in Alertmanager receiver integrations.")
	f.BoolVar(&l.AlertmanagerReceiversBlockPrivateAddresses, "alertmanager.receivers-firewall-block-private-addresses", false, "True to block private and local addresses in Alertmanager receiver integrations. It blocks private addresses defined by  RFC 1918 (IPv4 addresses) and RFC 4193 (IPv6 addresses), as well as loopback, local unicast and local multicast addresses.")

	f.Float64Var(&l.NotificationRateLimit, "alertmanager.notification-rate-limit", 0, "Per-user rate limit for sending notifications from Alertmanager in notifications/sec. 0 = rate limit disabled. Negative value = no notifications are allowed.")

	if l.NotificationRateLimitPerIntegration == nil {
		l.NotificationRateLimitPerIntegration = NotificationRateLimitMap{}
	}
	f.Var(&l.NotificationRateLimitPerIntegration, "alertmanager.notification-rate-limit-per-integration", "Per-integration notification rate limits. Value is a map, where each key is integration name and value is a rate-limit (float). On command line, this map is given in JSON format. Rate limit has the same meaning as -alertmanager.notification-rate-limit, but only applies for specific integration. Allowed integration names: "+strings.Join(allowedIntegrationNames, ", ")+".")
	f.IntVar(&l.AlertmanagerMaxConfigSizeBytes, "alertmanager.max-config-size-bytes", 0, "Maximum size of configuration file for Alertmanager that tenant can upload via Alertmanager API. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxTemplatesCount, "alertmanager.max-templates-count", 0, "Maximum number of templates in tenant's Alertmanager configuration uploaded via Alertmanager API. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxTemplateSizeBytes, "alertmanager.max-template-size-bytes", 0, "Maximum size of single template in tenant's Alertmanager configuration uploaded via Alertmanager API. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxDispatcherAggregationGroups, "alertmanager.max-dispatcher-aggregation-groups", 0, "Maximum number of aggregation groups in Alertmanager's dispatcher that a tenant can have. Each active aggregation group uses single goroutine. When the limit is reached, dispatcher will not dispatch alerts that belong to additional aggregation groups, but existing groups will keep working properly. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxAlertsCount, "alertmanager.max-alerts-count", 0, "Maximum number of alerts that a single user can have. Inserting more alerts will fail with a log message and metric increment. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxAlertsSizeBytes, "alertmanager.max-alerts-size-bytes", 0, "Maximum total size of alerts that a single user can have, alert size is the sum of the bytes of its labels, annotations and generatorURL. Inserting more alerts will fail with a log message and metric increment. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxSilencesCount, "alertmanager.max-silences-count", 0, "Maximum number of silences that a single user can have, including expired silences. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxSilencesSizeBytes, "alertmanager.max-silences-size-bytes", 0, "Maximum size of individual silences that a single user can have. 0 = no limit.")
}

// Validate the limits config and returns an error if the validation
// doesn't pass
func (l *Limits) Validate(shardByAllLabels bool) error {
	// The ingester.max-global-series-per-user metric is not supported
	// if shard-by-all-labels is disabled
	if l.MaxGlobalSeriesPerUser > 0 && !shardByAllLabels {
		return errMaxGlobalSeriesPerUserValidation
	}

	if err := l.RulerExternalLabels.Validate(func(l labels.Label) error {
		if !model.LabelName(l.Name).IsValid() {
			return fmt.Errorf("%w: %q", errInvalidLabelName, l.Name)
		}
		if !model.LabelValue(l.Value).IsValid() {
			return fmt.Errorf("%w: %q", errInvalidLabelValue, l.Value)
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (l *Limits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// We want to set l to the defaults and then overwrite it with the input.
	// To make unmarshal fill the plain data struct rather than calling UnmarshalYAML
	// again, we have to hide it using a type indirection.  See prometheus/config.

	// During startup we won't have a default value so we don't want to overwrite them
	if defaultLimits != nil {
		*l = *defaultLimits
		// Make copy of default limits. Otherwise unmarshalling would modify map in default limits.
		l.copyNotificationIntegrationLimits(defaultLimits.NotificationRateLimitPerIntegration)
	}
	type plain Limits
	if err := unmarshal((*plain)(l)); err != nil {
		return err
	}

	if err := l.compileQueryAttributeRegex(); err != nil {
		return err
	}

	if err := l.calculateMaxSeriesPerLabelSetId(); err != nil {
		return err
	}

	return nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (l *Limits) UnmarshalJSON(data []byte) error {
	// Like the YAML method above, we want to set l to the defaults and then overwrite
	// it with the input. We prevent an infinite loop of calling UnmarshalJSON by hiding
	// behind type indirection.
	if defaultLimits != nil {
		*l = *defaultLimits
		// Make copy of default limits. Otherwise unmarshalling would modify map in default limits.
		l.copyNotificationIntegrationLimits(defaultLimits.NotificationRateLimitPerIntegration)
	}

	type plain Limits
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	if err := dec.Decode((*plain)(l)); err != nil {
		return err
	}

	if err := l.compileQueryAttributeRegex(); err != nil {
		return err
	}

	if err := l.calculateMaxSeriesPerLabelSetId(); err != nil {
		return err
	}

	return nil
}

func (l *Limits) calculateMaxSeriesPerLabelSetId() error {
	hMap := map[uint64]struct{}{}

	for k, limit := range l.LimitsPerLabelSet {
		limit.Id = limit.LabelSet.String()
		limit.Hash = fnv1a.HashBytes64([]byte(limit.Id))
		l.LimitsPerLabelSet[k] = limit
		if _, ok := hMap[limit.Hash]; ok {
			return errDuplicatePerLabelSetLimit
		}
		hMap[limit.Hash] = struct{}{}
	}

	return nil
}

func (l *Limits) copyNotificationIntegrationLimits(defaults NotificationRateLimitMap) {
	l.NotificationRateLimitPerIntegration = make(map[string]float64, len(defaults))
	for k, v := range defaults {
		l.NotificationRateLimitPerIntegration[k] = v
	}
}

func (l *Limits) hasQueryAttributeRegexChanged() bool {
	var newHash uint64
	h := xxhash.New()

	if l.QueryPriority.Enabled {
		for _, priority := range l.QueryPriority.Priorities {
			for _, attribute := range priority.QueryAttributes {
				addToHash(h, attribute.Regex)
				addToHash(h, attribute.UserAgentRegex)
			}
		}
	}
	if l.QueryRejection.Enabled {
		for _, attribute := range l.QueryRejection.QueryAttributes {
			addToHash(h, attribute.Regex)
			addToHash(h, attribute.UserAgentRegex)
		}
	}

	newHash = h.Sum64()

	if newHash != l.queryAttributeRegexHash {
		l.queryAttributeRegexHash = newHash
		return true
	}
	return false
}

func addToHash(h *xxhash.Digest, regex string) {
	if regex == "" {
		return
	}
	_, _ = h.WriteString(regex)
	_, _ = h.Write([]byte{'\xff'})
}

func (l *Limits) compileQueryAttributeRegex() error {
	if !l.QueryPriority.Enabled && !l.QueryRejection.Enabled {
		return nil
	}
	regexChanged := l.hasQueryAttributeRegexChanged()
	newCompiledRegex := map[string]*regexp.Regexp{}

	if l.QueryPriority.Enabled {
		prioritySet := map[int64]struct{}{}

		for i, priority := range l.QueryPriority.Priorities {
			// Check for duplicate priority entry
			if _, exists := prioritySet[priority.Priority]; exists {
				return errDuplicateQueryPriorities
			}
			prioritySet[priority.Priority] = struct{}{}

			err := l.compileQueryAttributeRegexes(l.QueryPriority.Priorities[i].QueryAttributes, regexChanged, newCompiledRegex)
			if err != nil {
				return err
			}
		}
	}

	if l.QueryRejection.Enabled {
		err := l.compileQueryAttributeRegexes(l.QueryRejection.QueryAttributes, regexChanged, newCompiledRegex)
		if err != nil {
			return err
		}
	}

	if regexChanged {
		l.queryAttributeCompiledRegex = newCompiledRegex
	}

	return nil
}

func (l *Limits) compileQueryAttributeRegexes(queryAttributes []QueryAttribute, regexChanged bool, newCompiledRegex map[string]*regexp.Regexp) error {
	for j, attribute := range queryAttributes {
		if regexChanged {
			compiledRegex, err := regexp.Compile(attribute.Regex)
			if err != nil {
				return errors.Join(errCompilingQueryPriorityRegex, err)
			}
			newCompiledRegex[attribute.Regex] = compiledRegex
			queryAttributes[j].CompiledRegex = compiledRegex

			compiledUserAgentRegex, err := regexp.Compile(attribute.UserAgentRegex)
			if err != nil {
				return errors.Join(errCompilingQueryPriorityRegex, err)
			}
			newCompiledRegex[attribute.UserAgentRegex] = compiledUserAgentRegex
			queryAttributes[j].CompiledUserAgentRegex = compiledUserAgentRegex
		} else {
			queryAttributes[j].CompiledRegex = l.queryAttributeCompiledRegex[attribute.Regex]
			queryAttributes[j].CompiledUserAgentRegex = l.queryAttributeCompiledRegex[attribute.UserAgentRegex]
		}
	}
	return nil
}

// When we load YAML from disk, we want the various per-customer limits
// to default to any values specified on the command line, not default
// command line values.  This global contains those values.  I (Tom) cannot
// find a nicer way I'm afraid.
var defaultLimits *Limits

// SetDefaultLimitsForYAMLUnmarshalling sets global default limits, used when loading
// Limits from YAML files. This is used to ensure per-tenant limits are defaulted to
// those values.
func SetDefaultLimitsForYAMLUnmarshalling(defaults Limits) {
	defaultLimits = &defaults
}

// TenantLimits exposes per-tenant limit overrides to various resource usage limits
type TenantLimits interface {
	// ByUserID gets limits specific to a particular tenant or nil if there are none
	ByUserID(userID string) *Limits

	// AllByUserID gets a mapping of all tenant IDs and limits for that user
	AllByUserID() map[string]*Limits
}

// Overrides periodically fetch a set of per-user overrides, and provides convenience
// functions for fetching the correct value.
type Overrides struct {
	defaultLimits *Limits
	tenantLimits  TenantLimits
}

// NewOverrides makes a new Overrides.
func NewOverrides(defaults Limits, tenantLimits TenantLimits) (*Overrides, error) {
	return &Overrides{
		tenantLimits:  tenantLimits,
		defaultLimits: &defaults,
	}, nil
}

// IngestionRate returns the limit on ingester rate (samples per second).
func (o *Overrides) IngestionRate(userID string) float64 {
	return o.GetOverridesForUser(userID).IngestionRate
}

// IngestionRateStrategy returns whether the ingestion rate limit should be individually applied
// to each distributor instance (local) or evenly shared across the cluster (global).
func (o *Overrides) IngestionRateStrategy() string {
	// The ingestion rate strategy can't be overridden on a per-tenant basis
	return o.defaultLimits.IngestionRateStrategy
}

// IngestionBurstSize returns the burst size for ingestion rate.
func (o *Overrides) IngestionBurstSize(userID string) int {
	return o.GetOverridesForUser(userID).IngestionBurstSize
}

// AcceptHASamples returns whether the distributor should track and accept samples from HA replicas for this user.
func (o *Overrides) AcceptHASamples(userID string) bool {
	return o.GetOverridesForUser(userID).AcceptHASamples
}

// AcceptMixedHASamples returns whether the distributor should track and accept samples from mixed HA replicas for this user.
func (o *Overrides) AcceptMixedHASamples(userID string) bool {
	return o.GetOverridesForUser(userID).AcceptMixedHASamples
}

// HAClusterLabel returns the cluster label to look for when deciding whether to accept a sample from a Prometheus HA replica.
func (o *Overrides) HAClusterLabel(userID string) string {
	return o.GetOverridesForUser(userID).HAClusterLabel
}

// HAReplicaLabel returns the replica label to look for when deciding whether to accept a sample from a Prometheus HA replica.
func (o *Overrides) HAReplicaLabel(userID string) string {
	return o.GetOverridesForUser(userID).HAReplicaLabel
}

// DropLabels returns the list of labels to be dropped when ingesting HA samples for the user.
func (o *Overrides) DropLabels(userID string) flagext.StringSlice {
	return o.GetOverridesForUser(userID).DropLabels
}

// MaxLabelNameLength returns maximum length a label name can be.
func (o *Overrides) MaxLabelNameLength(userID string) int {
	return o.GetOverridesForUser(userID).MaxLabelNameLength
}

// MaxLabelValueLength returns maximum length a label value can be. This also is
// the maximum length of a metric name.
func (o *Overrides) MaxLabelValueLength(userID string) int {
	return o.GetOverridesForUser(userID).MaxLabelValueLength
}

// MaxLabelNamesPerSeries returns maximum number of label/value pairs timeseries.
func (o *Overrides) MaxLabelNamesPerSeries(userID string) int {
	return o.GetOverridesForUser(userID).MaxLabelNamesPerSeries
}

// MaxLabelsSizeBytes returns maximum number of label/value pairs timeseries.
func (o *Overrides) MaxLabelsSizeBytes(userID string) int {
	return o.GetOverridesForUser(userID).MaxLabelsSizeBytes
}

// MaxMetadataLength returns maximum length metadata can be. Metadata refers
// to the Metric Name, HELP and UNIT.
func (o *Overrides) MaxMetadataLength(userID string) int {
	return o.GetOverridesForUser(userID).MaxMetadataLength
}

// RejectOldSamples returns true when we should reject samples older than certain
// age.
func (o *Overrides) RejectOldSamples(userID string) bool {
	return o.GetOverridesForUser(userID).RejectOldSamples
}

// RejectOldSamplesMaxAge returns the age at which samples should be rejected.
func (o *Overrides) RejectOldSamplesMaxAge(userID string) time.Duration {
	return time.Duration(o.GetOverridesForUser(userID).RejectOldSamplesMaxAge)
}

// CreationGracePeriod is misnamed, and actually returns how far into the future
// we should accept samples.
func (o *Overrides) CreationGracePeriod(userID string) time.Duration {
	return time.Duration(o.GetOverridesForUser(userID).CreationGracePeriod)
}

// MaxLocalSeriesPerUser returns the maximum number of series a user is allowed to store in a single ingester.
func (o *Overrides) MaxLocalSeriesPerUser(userID string) int {
	return o.GetOverridesForUser(userID).MaxLocalSeriesPerUser
}

// MaxLocalSeriesPerMetric returns the maximum number of series allowed per metric in a single ingester.
func (o *Overrides) MaxLocalSeriesPerMetric(userID string) int {
	return o.GetOverridesForUser(userID).MaxLocalSeriesPerMetric
}

// MaxGlobalSeriesPerUser returns the maximum number of series a user is allowed to store across the cluster.
func (o *Overrides) MaxGlobalSeriesPerUser(userID string) int {
	return o.GetOverridesForUser(userID).MaxGlobalSeriesPerUser
}

// OutOfOrderTimeWindow returns the allowed time window for ingestion of out-of-order samples.
func (o *Overrides) OutOfOrderTimeWindow(userID string) model.Duration {
	return o.GetOverridesForUser(userID).OutOfOrderTimeWindow
}

// MaxGlobalSeriesPerMetric returns the maximum number of series allowed per metric across the cluster.
func (o *Overrides) MaxGlobalSeriesPerMetric(userID string) int {
	return o.GetOverridesForUser(userID).MaxGlobalSeriesPerMetric
}

// LimitsPerLabelSet returns the user limits per labelset across the cluster.
func (o *Overrides) LimitsPerLabelSet(userID string) []LimitsPerLabelSet {
	return o.GetOverridesForUser(userID).LimitsPerLabelSet
}

// MaxChunksPerQueryFromStore returns the maximum number of chunks allowed per query when fetching
// chunks from the long-term storage.
func (o *Overrides) MaxChunksPerQueryFromStore(userID string) int {
	return o.GetOverridesForUser(userID).MaxChunksPerQuery
}

func (o *Overrides) MaxChunksPerQuery(userID string) int {
	return o.GetOverridesForUser(userID).MaxChunksPerQuery
}

// MaxFetchedSeriesPerQuery returns the maximum number of series allowed per query when fetching
// chunks from ingesters and blocks storage.
func (o *Overrides) MaxFetchedSeriesPerQuery(userID string) int {
	return o.GetOverridesForUser(userID).MaxFetchedSeriesPerQuery
}

// MaxFetchedChunkBytesPerQuery returns the maximum number of bytes for chunks allowed per query when fetching
// chunks from ingesters and blocks storage.
func (o *Overrides) MaxFetchedChunkBytesPerQuery(userID string) int {
	return o.GetOverridesForUser(userID).MaxFetchedChunkBytesPerQuery
}

// MaxFetchedDataBytesPerQuery returns the maximum number of bytes for all data allowed per query when fetching
// from ingesters and blocks storage.
func (o *Overrides) MaxFetchedDataBytesPerQuery(userID string) int {
	return o.GetOverridesForUser(userID).MaxFetchedDataBytesPerQuery
}

// MaxDownloadedBytesPerRequest returns the maximum number of bytes to download for each gRPC request in Store Gateway,
// including any data fetched from cache or object storage.
func (o *Overrides) MaxDownloadedBytesPerRequest(userID string) int {
	return o.GetOverridesForUser(userID).MaxDownloadedBytesPerRequest
}

// MaxQueryLookback returns the max lookback period of queries.
func (o *Overrides) MaxQueryLookback(userID string) time.Duration {
	return time.Duration(o.GetOverridesForUser(userID).MaxQueryLookback)
}

// MaxQueryLength returns the limit of the length (in time) of a query.
func (o *Overrides) MaxQueryLength(userID string) time.Duration {
	return time.Duration(o.GetOverridesForUser(userID).MaxQueryLength)
}

// MaxCacheFreshness returns the period after which results are cacheable,
// to prevent caching of very recent results.
func (o *Overrides) MaxCacheFreshness(userID string) time.Duration {
	return time.Duration(o.GetOverridesForUser(userID).MaxCacheFreshness)
}

// MaxQueriersPerUser returns the maximum number of queriers that can handle requests for this user.
func (o *Overrides) MaxQueriersPerUser(userID string) float64 {
	return o.GetOverridesForUser(userID).MaxQueriersPerTenant
}

// QueryVerticalShardSize returns the number of shards to use when distributing shardable PromQL queries.
func (o *Overrides) QueryVerticalShardSize(userID string) int {
	return o.GetOverridesForUser(userID).QueryVerticalShardSize
}

// QueryPartialData returns whether query may be evaluated with data from a single zone, if other zones are not available.
func (o *Overrides) QueryPartialData(userID string) bool {
	return o.GetOverridesForUser(userID).QueryPartialData
}

// MaxQueryParallelism returns the limit to the number of split queries the
// frontend will process in parallel.
func (o *Overrides) MaxQueryParallelism(userID string) int {
	return o.GetOverridesForUser(userID).MaxQueryParallelism
}

// MaxOutstandingPerTenant returns the limit to the maximum number
// of outstanding requests per tenant per request queue.
func (o *Overrides) MaxOutstandingPerTenant(userID string) int {
	return o.GetOverridesForUser(userID).MaxOutstandingPerTenant
}

// QueryPriority returns the query priority config for the tenant, including different priorities and their attributes
func (o *Overrides) QueryPriority(userID string) QueryPriority {
	return o.GetOverridesForUser(userID).QueryPriority
}

// QueryRejection returns the query reject config for the tenant
func (o *Overrides) QueryRejection(userID string) QueryRejection {
	return o.GetOverridesForUser(userID).QueryRejection
}

// EnforceMetricName whether to enforce the presence of a metric name.
func (o *Overrides) EnforceMetricName(userID string) bool {
	return o.GetOverridesForUser(userID).EnforceMetricName
}

// EnforceMetadataMetricName whether to enforce the presence of a metric name on metadata.
func (o *Overrides) EnforceMetadataMetricName(userID string) bool {
	return o.GetOverridesForUser(userID).EnforceMetadataMetricName
}

// MaxNativeHistogramBuckets returns the maximum total number of positive and negative buckets of a single native histogram
// a user is allowed to store.
func (o *Overrides) MaxNativeHistogramBuckets(userID string) int {
	return o.GetOverridesForUser(userID).MaxNativeHistogramBuckets
}

// MaxLocalMetricsWithMetadataPerUser returns the maximum number of metrics with metadata a user is allowed to store in a single ingester.
func (o *Overrides) MaxLocalMetricsWithMetadataPerUser(userID string) int {
	return o.GetOverridesForUser(userID).MaxLocalMetricsWithMetadataPerUser
}

// MaxLocalMetadataPerMetric returns the maximum number of metadata allowed per metric in a single ingester.
func (o *Overrides) MaxLocalMetadataPerMetric(userID string) int {
	return o.GetOverridesForUser(userID).MaxLocalMetadataPerMetric
}

// MaxGlobalMetricsWithMetadataPerUser returns the maximum number of metrics with metadata a user is allowed to store across the cluster.
func (o *Overrides) MaxGlobalMetricsWithMetadataPerUser(userID string) int {
	return o.GetOverridesForUser(userID).MaxGlobalMetricsWithMetadataPerUser
}

// MaxGlobalMetadataPerMetric returns the maximum number of metadata allowed per metric across the cluster.
func (o *Overrides) MaxGlobalMetadataPerMetric(userID string) int {
	return o.GetOverridesForUser(userID).MaxGlobalMetadataPerMetric
}

// PromoteResourceAttributes returns the promote resource attributes for a given user.
func (o *Overrides) PromoteResourceAttributes(userID string) []string {
	return o.GetOverridesForUser(userID).PromoteResourceAttributes
}

// IngestionTenantShardSize returns the ingesters shard size for a given user.
func (o *Overrides) IngestionTenantShardSize(userID string) int {
	return o.GetOverridesForUser(userID).IngestionTenantShardSize
}

// CompactorBlocksRetentionPeriod returns the retention period for a given user.
func (o *Overrides) CompactorBlocksRetentionPeriod(userID string) time.Duration {
	return time.Duration(o.GetOverridesForUser(userID).CompactorBlocksRetentionPeriod)
}

// CompactorTenantShardSize returns shard size (number of rulers) used by this tenant when using shuffle-sharding strategy.
func (o *Overrides) CompactorTenantShardSize(userID string) int {
	return o.GetOverridesForUser(userID).CompactorTenantShardSize
}

// CompactorPartitionIndexSizeBytes returns shard size (number of rulers) used by this tenant when using shuffle-sharding strategy.
func (o *Overrides) CompactorPartitionIndexSizeBytes(userID string) int64 {
	return o.GetOverridesForUser(userID).CompactorPartitionIndexSizeBytes
}

// CompactorPartitionSeriesCount returns shard size (number of rulers) used by this tenant when using shuffle-sharding strategy.
func (o *Overrides) CompactorPartitionSeriesCount(userID string) int64 {
	return o.GetOverridesForUser(userID).CompactorPartitionSeriesCount
}

// MetricRelabelConfigs returns the metric relabel configs for a given user.
func (o *Overrides) MetricRelabelConfigs(userID string) []*relabel.Config {
	return o.GetOverridesForUser(userID).MetricRelabelConfigs
}

// RulerTenantShardSize returns shard size (number of rulers) used by this tenant when using shuffle-sharding strategy.
func (o *Overrides) RulerTenantShardSize(userID string) int {
	return o.GetOverridesForUser(userID).RulerTenantShardSize
}

// RulerMaxRulesPerRuleGroup returns the maximum number of rules per rule group for a given user.
func (o *Overrides) RulerMaxRulesPerRuleGroup(userID string) int {
	return o.GetOverridesForUser(userID).RulerMaxRulesPerRuleGroup
}

// RulerMaxRuleGroupsPerTenant returns the maximum number of rule groups for a given user.
func (o *Overrides) RulerMaxRuleGroupsPerTenant(userID string) int {
	return o.GetOverridesForUser(userID).RulerMaxRuleGroupsPerTenant
}

// RulerQueryOffset returns the rule query offset for a given user.
func (o *Overrides) RulerQueryOffset(userID string) time.Duration {
	ruleOffset := time.Duration(o.GetOverridesForUser(userID).RulerQueryOffset)
	evaluationDelay := time.Duration(o.GetOverridesForUser(userID).RulerEvaluationDelay)
	if evaluationDelay > ruleOffset {
		level.Warn(util_log.Logger).Log("msg", "ruler.query-offset was overridden by highest value in [Deprecated]ruler.evaluation-delay-duration", "ruler.query-offset", ruleOffset, "ruler.evaluation-delay-duration", evaluationDelay)
		return evaluationDelay
	}
	return ruleOffset
}

// RulesPartialData returns whether rule may be evaluated with data from a single zone, if other zones are not available.
func (o *Overrides) RulesPartialData(userID string) bool {
	return o.GetOverridesForUser(userID).RulesPartialData
}

// StoreGatewayTenantShardSize returns the store-gateway shard size for a given user.
func (o *Overrides) StoreGatewayTenantShardSize(userID string) float64 {
	return o.GetOverridesForUser(userID).StoreGatewayTenantShardSize
}

// MaxHAReplicaGroups returns maximum number of clusters that HA tracker will track for a user.
func (o *Overrides) MaxHAReplicaGroups(user string) int {
	return o.GetOverridesForUser(user).HAMaxClusters
}

// S3SSEType returns the per-tenant S3 SSE type.
func (o *Overrides) S3SSEType(user string) string {
	return o.GetOverridesForUser(user).S3SSEType
}

// S3SSEKMSKeyID returns the per-tenant S3 KMS-SSE key id.
func (o *Overrides) S3SSEKMSKeyID(user string) string {
	return o.GetOverridesForUser(user).S3SSEKMSKeyID
}

// S3SSEKMSEncryptionContext returns the per-tenant S3 KMS-SSE encryption context.
func (o *Overrides) S3SSEKMSEncryptionContext(user string) string {
	return o.GetOverridesForUser(user).S3SSEKMSEncryptionContext
}

// AlertmanagerReceiversBlockCIDRNetworks returns the list of network CIDRs that should be blocked
// in the Alertmanager receivers for the given user.
func (o *Overrides) AlertmanagerReceiversBlockCIDRNetworks(user string) []flagext.CIDR {
	return o.GetOverridesForUser(user).AlertmanagerReceiversBlockCIDRNetworks
}

// AlertmanagerReceiversBlockPrivateAddresses returns true if private addresses should be blocked
// in the Alertmanager receivers for the given user.
func (o *Overrides) AlertmanagerReceiversBlockPrivateAddresses(user string) bool {
	return o.GetOverridesForUser(user).AlertmanagerReceiversBlockPrivateAddresses
}

// MaxExemplars gets the maximum number of exemplars that will be stored per user. 0 or less means disabled.
func (o *Overrides) MaxExemplars(userID string) int {
	return o.GetOverridesForUser(userID).MaxExemplars
}

// Notification limits are special. Limits are returned in following order:
// 1. per-tenant limits for given integration
// 2. default limits for given integration
// 3. per-tenant limits
// 4. default limits
func (o *Overrides) getNotificationLimitForUser(user, integration string) float64 {
	u := o.GetOverridesForUser(user)
	if n, ok := u.NotificationRateLimitPerIntegration[integration]; ok {
		return n
	}

	return u.NotificationRateLimit
}

func (o *Overrides) NotificationRateLimit(user string, integration string) rate.Limit {
	l := o.getNotificationLimitForUser(user, integration)
	if l == 0 || math.IsInf(l, 1) {
		return rate.Inf // No rate limit.
	}

	if l < 0 {
		l = 0 // No notifications will be sent.
	}
	return rate.Limit(l)
}

const maxInt = int(^uint(0) >> 1)

func (o *Overrides) NotificationBurstSize(user string, integration string) int {
	// Burst size is computed from rate limit. Rate limit is already normalized to [0, +inf), where 0 means disabled.
	l := o.NotificationRateLimit(user, integration)
	if l == 0 {
		return 0
	}

	// floats can be larger than max int. This also handles case where l == rate.Inf.
	if float64(l) >= float64(maxInt) {
		return maxInt
	}

	// For values between (0, 1), allow single notification per second (every 1/limit seconds).
	if l < 1 {
		return 1
	}

	return int(l)
}

func (o *Overrides) AlertmanagerMaxConfigSize(userID string) int {
	return o.GetOverridesForUser(userID).AlertmanagerMaxConfigSizeBytes
}

func (o *Overrides) AlertmanagerMaxTemplatesCount(userID string) int {
	return o.GetOverridesForUser(userID).AlertmanagerMaxTemplatesCount
}

func (o *Overrides) AlertmanagerMaxTemplateSize(userID string) int {
	return o.GetOverridesForUser(userID).AlertmanagerMaxTemplateSizeBytes
}

func (o *Overrides) AlertmanagerMaxDispatcherAggregationGroups(userID string) int {
	return o.GetOverridesForUser(userID).AlertmanagerMaxDispatcherAggregationGroups
}

func (o *Overrides) AlertmanagerMaxAlertsCount(userID string) int {
	return o.GetOverridesForUser(userID).AlertmanagerMaxAlertsCount
}

func (o *Overrides) AlertmanagerMaxAlertsSizeBytes(userID string) int {
	return o.GetOverridesForUser(userID).AlertmanagerMaxAlertsSizeBytes
}

func (o *Overrides) AlertmanagerMaxSilencesCount(userID string) int {
	return o.GetOverridesForUser(userID).AlertmanagerMaxSilencesCount
}

func (o *Overrides) AlertmanagerMaxSilenceSizeBytes(userID string) int {
	return o.GetOverridesForUser(userID).AlertmanagerMaxSilencesSizeBytes
}

func (o *Overrides) DisabledRuleGroups(userID string) DisabledRuleGroups {
	if o.tenantLimits != nil {
		l := o.tenantLimits.ByUserID(userID)
		if l != nil {
			disabledRuleGroupsForUser := make(DisabledRuleGroups, len(l.DisabledRuleGroups))

			for i, disabledRuleGroup := range l.DisabledRuleGroups {
				disabledRuleGroupForUser := DisabledRuleGroup{
					Namespace: disabledRuleGroup.Namespace,
					Name:      disabledRuleGroup.Name,
					User:      userID,
				}
				disabledRuleGroupsForUser[i] = disabledRuleGroupForUser
			}
			return disabledRuleGroupsForUser
		}
	}
	return DisabledRuleGroups{}
}

func (o *Overrides) RulerExternalLabels(userID string) labels.Labels {
	return o.GetOverridesForUser(userID).RulerExternalLabels
}

// GetOverridesForUser returns the per-tenant limits with overrides.
func (o *Overrides) GetOverridesForUser(userID string) *Limits {
	if o.tenantLimits != nil {
		l := o.tenantLimits.ByUserID(userID)
		if l != nil {
			return l
		}
	}
	return o.defaultLimits
}

// SmallestPositiveIntPerTenant is returning the minimal positive value of the
// supplied limit function for all given tenants.
func SmallestPositiveIntPerTenant(tenantIDs []string, f func(string) int) int {
	var result *int
	for _, tenantID := range tenantIDs {
		v := f(tenantID)
		if result == nil || v < *result {
			result = &v
		}
	}
	if result == nil {
		return 0
	}
	return *result
}

// SmallestPositiveNonZeroFloat64PerTenant is returning the minimal positive and
// non-zero value of the supplied limit function for all given tenants. In many
// limits a value of 0 means unlimited so the method will return 0 only if all
// inputs have a limit of 0 or an empty tenant list is given.
func SmallestPositiveNonZeroFloat64PerTenant(tenantIDs []string, f func(string) float64) float64 {
	var result *float64
	for _, tenantID := range tenantIDs {
		v := f(tenantID)
		if v > 0 && (result == nil || v < *result) {
			result = &v
		}
	}
	if result == nil {
		return 0
	}
	return *result
}

// SmallestPositiveNonZeroDurationPerTenant is returning the minimal positive
// and non-zero value of the supplied limit function for all given tenants. In
// many limits a value of 0 means unlimited so the method will return 0 only if
// all inputs have a limit of 0 or an empty tenant list is given.
func SmallestPositiveNonZeroDurationPerTenant(tenantIDs []string, f func(string) time.Duration) time.Duration {
	var result *time.Duration
	for _, tenantID := range tenantIDs {
		v := f(tenantID)
		if v > 0 && (result == nil || v < *result) {
			result = &v
		}
	}
	if result == nil {
		return 0
	}
	return *result
}

// MaxDurationPerTenant is returning the maximum duration per tenant. Without
// tenants given it will return a time.Duration(0).
func MaxDurationPerTenant(tenantIDs []string, f func(string) time.Duration) time.Duration {
	result := time.Duration(0)
	for _, tenantID := range tenantIDs {
		v := f(tenantID)
		if v > result {
			result = v
		}
	}
	return result
}

// LimitsPerLabelSetsForSeries checks matching labelset limits for the given series.
func LimitsPerLabelSetsForSeries(limitsPerLabelSets []LimitsPerLabelSet, metric labels.Labels) []LimitsPerLabelSet {
	// returning early to not have any overhead
	if len(limitsPerLabelSets) == 0 {
		return nil
	}
	r := make([]LimitsPerLabelSet, 0, len(limitsPerLabelSets))
	defaultPartitionIndex := -1
outer:
	for i, lbls := range limitsPerLabelSets {
		// Default partition exists.
		if lbls.LabelSet.Len() == 0 {
			defaultPartitionIndex = i
			continue
		}
		for _, lbl := range lbls.LabelSet {
			// We did not find some of the labels on the set
			if v := metric.Get(lbl.Name); v != lbl.Value {
				continue outer
			}
		}
		r = append(r, lbls)
	}
	// Use default partition limiter if it is configured and no other matching partitions.
	if defaultPartitionIndex != -1 && len(r) == 0 {
		r = append(r, limitsPerLabelSets[defaultPartitionIndex])
	}
	return r
}
