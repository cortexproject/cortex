module github.com/cortexproject/cortex

go 1.12

require (
	cloud.google.com/go/bigtable v1.1.0
	cloud.google.com/go/storage v1.3.0
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d
	github.com/armon/go-metrics v0.3.0
	github.com/aws/aws-sdk-go v1.25.35
	github.com/blang/semver v3.5.0+incompatible
	github.com/bradfitz/gomemcache v0.0.0-20190329173943-551aad21a668
	github.com/cenkalti/backoff v1.0.0 // indirect
	github.com/cespare/xxhash v1.1.0
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20181012123002-c6f51f82210d // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fluent/fluent-logger-golang v1.2.1 // indirect
	github.com/fsouza/fake-gcs-server v1.7.0
	github.com/go-kit/kit v0.9.0
	github.com/gocql/gocql v0.0.0-20190301043612-f6df8288f9b4
	github.com/gogo/googleapis v1.1.0 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.0.3
	github.com/golang-migrate/migrate/v4 v4.7.0
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.1
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/gorilla/mux v1.7.1
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/hashicorp/consul/api v1.3.0
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.1.5
	github.com/json-iterator/go v1.1.8
	github.com/kylelemons/godebug v0.0.0-20170820004349-d65d576e9348 // indirect
	github.com/lann/builder v0.0.0-20150808151131-f22ce00fd939 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/lib/pq v1.0.0
	github.com/oklog/ulid v1.3.1
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing-contrib/go-stdlib v0.0.0-20190519235532-cf7a6c988dc9
	github.com/opentracing/opentracing-go v1.1.0
	github.com/philhofer/fwd v0.0.0-20160129035939-98c11a7a6ec8 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/alertmanager v0.19.0
	github.com/prometheus/client_golang v1.2.1
	github.com/prometheus/common v0.7.0
	github.com/prometheus/prometheus v1.8.2-0.20191126064551-80ba03c67da1
	github.com/rafaeljusto/redigomock v0.0.0-20190202135759-257e089e14a1
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sercand/kuberesolver v2.1.0+incompatible // indirect
	github.com/spf13/afero v1.2.2
	github.com/stretchr/testify v1.4.0
	github.com/thanos-io/thanos v0.8.1-0.20200102143048-a37ac093a67a
	github.com/tinylib/msgp v0.0.0-20161221055906-38a6f61a768d // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/uber-go/atomic v1.4.0
	github.com/uber/jaeger-client-go v2.20.1+incompatible
	github.com/weaveworks/billing-client v0.0.0-20171006123215-be0d55e547b1
	github.com/weaveworks/common v0.0.0-20190822150010-afb9996716e4
	github.com/weaveworks/promrus v1.2.0 // indirect
	go.etcd.io/bbolt v1.3.3
	go.etcd.io/etcd v0.0.0-20190709142735-eb7dd97135a5
	golang.org/x/net v0.0.0-20191112182307-2180aed22343
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/api v0.14.0
	google.golang.org/grpc v1.25.1
	gopkg.in/yaml.v2 v2.2.5
)

replace github.com/Azure/azure-sdk-for-go => github.com/Azure/azure-sdk-for-go v36.2.0+incompatible

replace github.com/Azure/go-autorest => github.com/Azure/go-autorest v13.3.0+incompatible

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

// Override reference that causes an error from Go proxy - see https://github.com/golang/go/issues/33558
replace k8s.io/client-go => k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
