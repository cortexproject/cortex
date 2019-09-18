module github.com/cortexproject/cortex

go 1.12

require (
	cloud.google.com/go v0.44.1
	github.com/Azure/azure-sdk-for-go v26.3.0+incompatible // indirect
	github.com/Azure/go-autorest v11.5.1+incompatible // indirect
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/aws/aws-sdk-go v1.22.4
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/blang/semver v3.5.0+incompatible
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/bradfitz/gomemcache v0.0.0-20190329173943-551aad21a668
	github.com/cenkalti/backoff v1.0.0 // indirect
	github.com/cespare/xxhash v1.1.0
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20181012123002-c6f51f82210d // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/cznic/ql v1.2.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fluent/fluent-logger-golang v1.2.1 // indirect
	github.com/fsouza/fake-gcs-server v1.3.0
	github.com/go-kit/kit v0.9.0
	github.com/gocql/gocql v0.0.0-20180113133114-697e7c57f99b
	github.com/gogo/googleapis v1.1.0 // indirect
	github.com/gogo/protobuf v1.2.2-0.20190730201129-28a6bbf47e48
	github.com/gogo/status v1.0.3
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.1
	github.com/gorilla/context v1.1.1 // indirect
	github.com/gorilla/mux v1.6.2
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/consul/api v1.1.0
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/jonboulle/clockwork v0.1.0
	github.com/json-iterator/go v1.1.7
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kylelemons/godebug v0.0.0-20170820004349-d65d576e9348 // indirect
	github.com/lann/builder v0.0.0-20150808151131-f22ce00fd939 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/lib/pq v1.0.0
	github.com/mattes/migrate v1.3.1
	github.com/mattn/go-sqlite3 v1.10.0 // indirect
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing-contrib/go-stdlib v0.0.0-20190519235532-cf7a6c988dc9
	github.com/opentracing/opentracing-go v1.1.0
	github.com/philhofer/fwd v0.0.0-20160129035939-98c11a7a6ec8 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/alertmanager v0.19.0
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/common v0.6.0
	github.com/prometheus/prometheus v0.0.0-20190818123050-43acd0e2e93f
	github.com/prometheus/tsdb v0.10.0
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sercand/kuberesolver v2.1.0+incompatible // indirect
	github.com/sirupsen/logrus v1.4.2 // indirect
	github.com/stretchr/testify v1.3.0
	github.com/tinylib/msgp v0.0.0-20161221055906-38a6f61a768d // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.0.0+incompatible // indirect
	github.com/weaveworks/billing-client v0.0.0-20171006123215-be0d55e547b1
	github.com/weaveworks/common v0.0.0-20190822150010-afb9996716e4
	github.com/weaveworks/promrus v1.2.0 // indirect
	go.etcd.io/bbolt v1.3.3
	go.etcd.io/etcd v0.0.0-20190709142735-eb7dd97135a5
	go.uber.org/zap v1.10.0 // indirect
	golang.org/x/net v0.0.0-20190724013045-ca1201d0de80
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/api v0.8.0
	google.golang.org/grpc v1.22.1
	gopkg.in/yaml.v2 v2.2.2
)

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

// Override reference that causes an error from Go proxy - see https://github.com/golang/go/issues/33558
replace k8s.io/client-go => k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
