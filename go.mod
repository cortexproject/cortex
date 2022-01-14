module github.com/cortexproject/cortex

go 1.16

require (
	cloud.google.com/go/bigtable v1.3.0
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20210927113745-59d0afb8317a
	github.com/alicebob/miniredis/v2 v2.14.3
	github.com/armon/go-metrics v0.3.9
	github.com/aws/aws-sdk-go v1.42.8
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/felixge/fgprof v0.9.1
	github.com/fsouza/fake-gcs-server v1.7.0
	github.com/go-kit/log v0.2.0
	github.com/go-openapi/strfmt v0.21.0
	github.com/go-openapi/swag v0.19.15
	github.com/go-redis/redis/v8 v8.11.4
	github.com/gocql/gocql v0.0.0-20200526081602-cd04bd7f22a7
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.0
	github.com/golang-migrate/migrate/v4 v4.7.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/btree v1.0.1 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/grafana/dskit v0.0.0-20220105080720-01ce9286d7d5
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/consul/api v1.11.0
	github.com/hashicorp/go-cleanhttp v0.5.2
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.2.4
	github.com/json-iterator/go v1.1.12
	github.com/lib/pq v1.3.0
	github.com/minio/minio-go/v7 v7.0.10
	github.com/mitchellh/go-wordwrap v1.0.0
	github.com/ncw/swift v1.0.52
	github.com/oklog/ulid v1.3.1
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/alertmanager v0.23.1-0.20210914172521-e35efbddb66a
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.32.1
	github.com/prometheus/prometheus v1.8.2-0.20211119115433-692a54649ed7
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sony/gobreaker v0.4.1
	github.com/spf13/afero v1.6.0
	github.com/stretchr/testify v1.7.0
	github.com/thanos-io/thanos v0.22.0
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/weaveworks/common v0.0.0-20210913144402-035033b78a78
	go.etcd.io/bbolt v1.3.6
	go.etcd.io/etcd v3.3.25+incompatible
	go.etcd.io/etcd/api/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0
	go.uber.org/atomic v1.9.0
	golang.org/x/net v0.0.0-20211020060615-d418f374d309
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/api v0.60.0
	google.golang.org/grpc v1.40.0
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

// TODO review the change introduced by https://github.com/grpc/grpc-go/pull/4416 before upgrading to 1.39.0
replace google.golang.org/grpc => google.golang.org/grpc v1.38.0

// We only pin this version to avoid problems with running go get: github.com/thanos-io/thanos@main. That
// currently fails because Thanos isn't merging release branches to main branch, and Go modules system is then
// confused about which version is the latest one. v0.22.0 was released in July, but latest tag reachable from main
// is v0.19.1. We pin version from early December here. Feel free to remove when updating to later version.
replace github.com/thanos-io/thanos v0.22.0 => github.com/thanos-io/thanos v0.19.1-0.20211208205607-d1acaea2a11a

// Pin aws-sdk to version prior to go-kit update, to reduce the bulk of change.
// Un-pin once Cortex 1.11 is released.
replace github.com/aws/aws-sdk-go => github.com/aws/aws-sdk-go v1.40.37
