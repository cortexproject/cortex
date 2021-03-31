module github.com/cortexproject/cortex

go 1.14

require (
	cloud.google.com/go/bigtable v1.2.0
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20210208195552-ff826a37aa15
	github.com/alicebob/miniredis v2.5.0+incompatible
	github.com/armon/go-metrics v0.3.6
	github.com/aws/aws-sdk-go v1.38.3
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/felixge/fgprof v0.9.1
	github.com/fsouza/fake-gcs-server v1.7.0
	github.com/go-kit/kit v0.10.0
	github.com/go-redis/redis/v8 v8.2.3
	github.com/gocql/gocql v0.0.0-20200526081602-cd04bd7f22a7
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.0.3
	github.com/golang-migrate/migrate/v4 v4.7.0
	github.com/golang/protobuf v1.4.3
	github.com/golang/snappy v0.0.3
	github.com/gorilla/mux v1.7.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.2
	github.com/hashicorp/consul/api v1.8.1
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.2.2
	github.com/json-iterator/go v1.1.10
	github.com/lib/pq v1.3.0
	github.com/minio/minio-go/v7 v7.0.10
	github.com/mitchellh/go-wordwrap v1.0.0
	github.com/ncw/swift v1.0.52
	github.com/oklog/ulid v1.3.1
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/alertmanager v0.21.1-0.20210310093010-0f9cab6991e6
	github.com/prometheus/client_golang v1.10.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.20.0
	github.com/prometheus/prometheus v1.8.2-0.20210324152458-c7a62b95cea0
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sony/gobreaker v0.4.1
	github.com/spf13/afero v1.2.2
	github.com/stretchr/testify v1.7.0
	github.com/thanos-io/thanos v0.13.1-0.20210226164558-03dace0a1aa1
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/weaveworks/common v0.0.0-20210112142934-23c8d7fa6120
	go.etcd.io/bbolt v1.3.5
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200520232829-54ba9589114f
	go.etcd.io/etcd/client/v3 v3.5.0-alpha.0.0.20210225194612-fa82d11a958a
	go.etcd.io/etcd/server/v3 v3.5.0-alpha.0.0.20210225194612-fa82d11a958a
	go.uber.org/atomic v1.7.0
	golang.org/x/net v0.0.0-20210324051636-2c4c8ecb7826
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/api v0.42.0
	google.golang.org/grpc v1.36.0
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	sigs.k8s.io/yaml v1.2.0
)

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

replace k8s.io/client-go => k8s.io/client-go v0.20.4

replace k8s.io/api => k8s.io/api v0.20.4

// >v1.2.0 has some conflict with prometheus/alertmanager. Hence prevent the upgrade till it's fixed.
replace github.com/satori/go.uuid => github.com/satori/go.uuid v1.2.0

// Use fork of gocql that has gokit logs and Prometheus metrics.
replace github.com/gocql/gocql => github.com/grafana/gocql v0.0.0-20200605141915-ba5dc39ece85

// Using a 3rd-party branch for custom dialer - see https://github.com/bradfitz/gomemcache/pull/86
replace github.com/bradfitz/gomemcache => github.com/themihai/gomemcache v0.0.0-20180902122335-24332e2d58ab

// Pin github.com/go-openapi versions to match Prometheus alertmanager to avoid
// breaking changing affecting the alertmanager.
replace github.com/go-openapi/errors => github.com/go-openapi/errors v0.19.4

replace github.com/go-openapi/loads => github.com/go-openapi/loads v0.19.5

replace github.com/go-openapi/runtime => github.com/go-openapi/runtime v0.19.15

replace github.com/go-openapi/spec => github.com/go-openapi/spec v0.19.8

replace github.com/go-openapi/strfmt => github.com/go-openapi/strfmt v0.19.5

replace github.com/go-openapi/swag => github.com/go-openapi/swag v0.19.9

replace github.com/go-openapi/validate => github.com/go-openapi/validate v0.19.8

// Pin these, which are updated as dependencies in Prometheus; we will take those updates separately and carefully
replace (
	github.com/aws/aws-sdk-go => github.com/aws/aws-sdk-go v1.37.8
	github.com/google/pprof => github.com/google/pprof v0.0.0-20210208152844-1612e9be7af6
	github.com/miekg/dns => github.com/miekg/dns v1.1.38
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v1.9.0
	golang.org/x/crypto => golang.org/x/crypto v0.0.0-20201208171446-5f87f3452ae9
	golang.org/x/net => golang.org/x/net v0.0.0-20210119194325-5f4716e94777
	golang.org/x/oauth2 => golang.org/x/oauth2 v0.0.0-20210210192628-66670185b0cd
	golang.org/x/sync => golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
	golang.org/x/sys => golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c
	google.golang.org/api => google.golang.org/api v0.39.0
	google.golang.org/grpc => google.golang.org/grpc v1.34.0
)
