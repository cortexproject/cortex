module github.com/cortexproject/cortex

go 1.14

require (
	cloud.google.com/go/bigtable v1.2.0
	cloud.google.com/go/storage v1.6.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/alicebob/miniredis v2.5.0+incompatible
	github.com/armon/go-metrics v0.3.3
	github.com/aws/aws-sdk-go v1.34.9
	github.com/blang/semver v3.5.0+incompatible
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/felixge/fgprof v0.9.1
	github.com/fsouza/fake-gcs-server v1.7.0
	github.com/go-kit/kit v0.10.0
	github.com/go-redis/redis/v8 v8.0.0-beta.10.0.20200905143926-df7fe4e2ce72
	github.com/gocql/gocql v0.0.0-20200526081602-cd04bd7f22a7
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.0.3
	github.com/golang-migrate/migrate/v4 v4.7.0
	github.com/golang/protobuf v1.4.2
	github.com/golang/snappy v0.0.1
	github.com/gorilla/mux v1.7.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/hashicorp/consul/api v1.6.0
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.2.2
	github.com/json-iterator/go v1.1.10
	github.com/lib/pq v1.3.0
	github.com/mitchellh/go-wordwrap v1.0.0
	github.com/ncw/swift v1.0.50
	github.com/oklog/ulid v1.3.1
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/alertmanager v0.21.1-0.20200911160112-1fdff6b3f939
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.14.0
	github.com/prometheus/prometheus v1.8.2-0.20200923143134-7e2db3d092f3
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sony/gobreaker v0.4.1
	github.com/spf13/afero v1.2.2
	github.com/stretchr/testify v1.6.1
	github.com/thanos-io/thanos v0.13.1-0.20200923175059-57035bf8f843
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/weaveworks/common v0.0.0-20200914083218-61ffdd448099
	github.com/yuin/gopher-lua v0.0.0-20200816102855-ee81675732da // indirect
	go.etcd.io/bbolt v1.3.5-0.20200615073812-232d8fc87f50
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200520232829-54ba9589114f
	go.uber.org/atomic v1.6.0
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e
	google.golang.org/api v0.29.0
	google.golang.org/grpc v1.30.0
	gopkg.in/yaml.v2 v2.3.0
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776
	sigs.k8s.io/yaml v1.2.0
)

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

replace k8s.io/client-go => k8s.io/client-go v0.18.3

// >v1.2.0 has some conflict with prometheus/alertmanager. Hence prevent the upgrade till it's fixed.
replace github.com/satori/go.uuid => github.com/satori/go.uuid v1.2.0

// Use fork of gocql that has gokit logs and Prometheus metrics.
replace github.com/gocql/gocql => github.com/grafana/gocql v0.0.0-20200605141915-ba5dc39ece85

// We can't upgrade to grpc 1.30.0 until go.etcd.io/etcd will support it.
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

// Using a 3rd-party branch for custom dialer - see https://github.com/bradfitz/gomemcache/pull/86
replace github.com/bradfitz/gomemcache => github.com/themihai/gomemcache v0.0.0-20180902122335-24332e2d58ab
