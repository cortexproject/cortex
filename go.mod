module github.com/cortexproject/cortex

go 1.16

require (
	cloud.google.com/go/bigtable v1.3.0
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20210208195552-ff826a37aa15
	github.com/alicebob/miniredis/v2 v2.14.3
	github.com/armon/go-metrics v0.3.6
	github.com/aws/aws-sdk-go v1.38.60
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/felixge/fgprof v0.9.1
	github.com/fsouza/fake-gcs-server v1.7.0
	github.com/go-kit/kit v0.10.0
	github.com/go-openapi/strfmt v0.20.1
	github.com/go-openapi/swag v0.19.15
	github.com/go-redis/redis/v8 v8.9.0
	github.com/gocql/gocql v0.0.0-20200526081602-cd04bd7f22a7
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.0.3
	github.com/golang-migrate/migrate/v4 v4.7.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.3
	github.com/gorilla/mux v1.7.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.2
	github.com/hashicorp/consul/api v1.8.1
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.2.3
	github.com/json-iterator/go v1.1.11
	github.com/lib/pq v1.3.0
	github.com/minio/minio-go/v7 v7.0.10
	github.com/mitchellh/go-wordwrap v1.0.0
	github.com/ncw/swift v1.0.52
	github.com/oklog/ulid v1.3.1
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/alertmanager v0.22.3-0.20210628111558-8491f816296b
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.29.0
	github.com/prometheus/prometheus v1.8.2-0.20210714080906-a0571449fd38
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sony/gobreaker v0.4.1
	github.com/spf13/afero v1.2.2
	github.com/stretchr/testify v1.7.0
	github.com/thanos-io/thanos v0.19.1-0.20210427154226-d5bd651319d2
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/weaveworks/common v0.0.0-20210419092856-009d1eebd624
	go.etcd.io/bbolt v1.3.5
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200520232829-54ba9589114f
	go.etcd.io/etcd/client/v3 v3.5.0-alpha.0.0.20210225194612-fa82d11a958a
	go.etcd.io/etcd/server/v3 v3.5.0-alpha.0.0.20210225194612-fa82d11a958a
	go.uber.org/atomic v1.8.0
	golang.org/x/net v0.0.0-20210610132358-84b48f89b13b
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210611083556-38a9dc6acbc6
	google.golang.org/api v0.48.0
	google.golang.org/grpc v1.38.0
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	sigs.k8s.io/yaml v1.2.0
)

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

replace k8s.io/client-go => k8s.io/client-go v0.20.4

replace k8s.io/api => k8s.io/api v0.20.4

// Use fork of gocql that has gokit logs and Prometheus metrics.
replace github.com/gocql/gocql => github.com/grafana/gocql v0.0.0-20200605141915-ba5dc39ece85

// Using a 3rd-party branch for custom dialer - see https://github.com/bradfitz/gomemcache/pull/86
replace github.com/bradfitz/gomemcache => github.com/themihai/gomemcache v0.0.0-20180902122335-24332e2d58ab
