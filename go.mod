module github.com/cortexproject/cortex

go 1.12

require (
	cloud.google.com/go v0.44.1
	github.com/Azure/azure-sdk-for-go v26.3.0+incompatible // indirect
	github.com/Azure/go-autorest v11.5.1+incompatible // indirect
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20190717042225-c3de453c63f4
	github.com/armon/go-metrics v0.0.0-20190430140413-ec5e00d3c878
	github.com/aws/aws-sdk-go v1.25.22
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
	github.com/gogo/protobuf v1.2.2-0.20190730201129-28a6bbf47e48
	github.com/gogo/status v1.0.3
	github.com/golang-migrate/migrate/v4 v4.7.0
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.1
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/gorilla/mux v1.7.1
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/hashicorp/consul/api v1.1.0
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.1.4
	github.com/json-iterator/go v1.1.7
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
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/common v0.7.0
	github.com/prometheus/prometheus v1.8.2-0.20190918104050-8744afdd1ea0
	github.com/rafaeljusto/redigomock v0.0.0-20190202135759-257e089e14a1
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sercand/kuberesolver v2.1.0+incompatible // indirect
	github.com/spf13/afero v1.2.2
	github.com/stretchr/testify v1.4.0
	github.com/thanos-io/thanos v0.8.1
	github.com/tinylib/msgp v0.0.0-20161221055906-38a6f61a768d // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/uber/jaeger-client-go v2.20.0+incompatible
	github.com/weaveworks/billing-client v0.0.0-20171006123215-be0d55e547b1
	github.com/weaveworks/common v0.0.0-20190822150010-afb9996716e4
	github.com/weaveworks/promrus v1.2.0 // indirect
	go.etcd.io/bbolt v1.3.3
	go.etcd.io/etcd v0.0.0-20190709142735-eb7dd97135a5
	golang.org/x/net v0.0.0-20190923162816-aa69164e4478
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/api v0.11.0
	google.golang.org/grpc v1.25.1
	gopkg.in/yaml.v2 v2.2.2
)

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

// Override reference that causes an error from Go proxy - see https://github.com/golang/go/issues/33558
replace k8s.io/client-go => k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
