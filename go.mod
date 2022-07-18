module github.com/cortexproject/cortex

go 1.16

require (
	cloud.google.com/go/bigtable v1.3.0
	cloud.google.com/go/iam v0.3.0 // indirect
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137
	github.com/alicebob/miniredis/v2 v2.14.3
	github.com/armon/go-metrics v0.3.9
	github.com/aws/aws-sdk-go v1.44.29
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/felixge/fgprof v0.9.2
	github.com/fsouza/fake-gcs-server v1.7.0
	github.com/go-kit/log v0.2.1
	github.com/go-openapi/strfmt v0.21.2
	github.com/go-openapi/swag v0.21.1
	github.com/go-redis/redis/v8 v8.11.4
	github.com/gocql/gocql v0.0.0-20200526081602-cd04bd7f22a7
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.0
	github.com/golang-migrate/migrate/v4 v4.7.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/gorilla/mux v1.8.0
	github.com/grafana/regexp v0.0.0-20220304095617-2e8d9baf4ac2
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/consul/api v1.13.0
	github.com/hashicorp/go-cleanhttp v0.5.2
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.3.1
	github.com/json-iterator/go v1.1.12
	github.com/lib/pq v1.3.0
	github.com/minio/minio-go/v7 v7.0.32-0.20220706200439-ef3e45ed9cdb
	github.com/mitchellh/go-wordwrap v1.0.0
	github.com/ncw/swift v1.0.52
	github.com/oklog/ulid v1.3.1
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/alertmanager v0.24.0
	github.com/prometheus/client_golang v1.12.2
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.34.0
	github.com/prometheus/prometheus v1.8.2-0.20220616150932-0d0e44d7e65f
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sony/gobreaker v0.4.1
	github.com/spf13/afero v1.6.0
	github.com/stretchr/testify v1.7.2
	github.com/thanos-io/thanos v0.27.0-rc.0.0.20220715025542-84880ea6d7f8
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/weaveworks/common v0.0.0-20210913144402-035033b78a78
	go.etcd.io/bbolt v1.3.6
	go.etcd.io/etcd/api/v3 v3.5.4
	go.etcd.io/etcd/client/pkg/v3 v3.5.4
	go.etcd.io/etcd/client/v3 v3.5.4
	go.uber.org/atomic v1.9.0
	golang.org/x/net v0.0.0-20220607020251-c690dde0001d
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f
	golang.org/x/time v0.0.0-20220224211638-0e9765cccd65
	google.golang.org/api v0.83.0
	google.golang.org/grpc v1.47.0
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	sigs.k8s.io/yaml v1.3.0
)

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

replace k8s.io/client-go => k8s.io/client-go v0.21.3

replace k8s.io/api => k8s.io/api v0.21.3

// Use fork of gocql that has gokit logs and Prometheus metrics.
replace github.com/gocql/gocql => github.com/grafana/gocql v0.0.0-20200605141915-ba5dc39ece85

// Using a 3rd-party branch for custom dialer - see https://github.com/bradfitz/gomemcache/pull/86
replace github.com/bradfitz/gomemcache => github.com/themihai/gomemcache v0.0.0-20180902122335-24332e2d58ab

// Replace memberlist with Grafana's fork which includes some fixes that haven't been merged upstream yet
replace github.com/hashicorp/memberlist => github.com/grafana/memberlist v0.2.5-0.20211201083710-c7bc8e9df94b

// Replace grpc as weaveworks common needs to update this first
// vendor/github.com/weaveworks/common/httpgrpc/server/server.go:137:3: undefined: grpc.WithBalancerName
replace google.golang.org/grpc => google.golang.org/grpc v1.45.0

// Same version being used by thanos
replace github.com/vimeo/galaxycache => github.com/thanos-community/galaxycache v0.0.0-20211122094458-3a32041a1f1e
