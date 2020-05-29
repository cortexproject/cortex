module github.com/cortexproject/cortex

go 1.13

require (
	cloud.google.com/go/bigtable v1.2.0
	cloud.google.com/go/storage v1.6.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d
	github.com/armon/go-metrics v0.3.3
	github.com/aws/aws-sdk-go v1.30.16
	github.com/blang/semver v3.5.0+incompatible
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fsouza/fake-gcs-server v1.7.0
	github.com/go-kit/kit v0.10.0
	github.com/gocql/gocql v0.0.0-20200121121104-95d072f1b5bb
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.0.3
	github.com/golang-migrate/migrate/v4 v4.7.0
	github.com/golang/protobuf v1.4.0
	github.com/golang/snappy v0.0.1
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/gorilla/mux v1.7.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/hashicorp/consul/api v1.4.0
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.2.0
	github.com/json-iterator/go v1.1.9
	github.com/lib/pq v1.3.0
	github.com/mitchellh/go-wordwrap v1.0.0
	github.com/ncw/swift v1.0.50
	github.com/oklog/ulid v1.3.1
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing-contrib/go-stdlib v0.0.0-20190519235532-cf7a6c988dc9
	github.com/opentracing/opentracing-go v1.1.1-0.20200124165624-2876d2018785
	github.com/pkg/errors v0.9.1
	github.com/prometheus/alertmanager v0.20.0
	github.com/prometheus/client_golang v1.6.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.10.0
	github.com/prometheus/prometheus v1.8.2-0.20200528145419-83619aa9ac56
	github.com/rafaeljusto/redigomock v0.0.0-20190202135759-257e089e14a1
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/spf13/afero v1.2.2
	github.com/stretchr/testify v1.5.1
	github.com/thanos-io/thanos v0.12.3-0.20200529100726-ce77c2d3b7d2
	github.com/uber/jaeger-client-go v2.23.0+incompatible
	github.com/weaveworks/common v0.0.0-20200512154658-384f10054ec5
	go.etcd.io/bbolt v1.3.4
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200520232829-54ba9589114f
	go.uber.org/atomic v1.6.0
	go.uber.org/zap v1.14.1 // indirect
	golang.org/x/net v0.0.0-20200421231249-e086a090c8fd
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
	golang.org/x/time v0.0.0-20200416051211-89c76fbcd5d1
	google.golang.org/api v0.22.0
	google.golang.org/grpc v1.29.0
	gopkg.in/yaml.v2 v2.2.8
	sigs.k8s.io/yaml v1.1.0
)

replace github.com/Azure/azure-sdk-for-go => github.com/Azure/azure-sdk-for-go v36.2.0+incompatible

replace github.com/Azure/go-autorest => github.com/Azure/go-autorest v13.3.0+incompatible

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

// Without this, it attempts to upgrade to v0.18.x which has some conflicts with upstream Prometheus.
// Also, v0.17.5 is chosen to be consistent with Thanos and more clear than using a commit hash.
replace k8s.io/client-go => k8s.io/client-go v0.17.5

// >v1.2.0 has some conflict with prometheus/alertmanager. Hence prevent the upgrade till it's fixed.
replace github.com/satori/go.uuid => github.com/satori/go.uuid v1.2.0
