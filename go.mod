module github.com/cortexproject/cortex

go 1.21

require (
	github.com/Masterminds/squirrel v1.5.4
	github.com/alecthomas/units v0.0.0-20231202071711-9a357b53e9c9
	github.com/alicebob/miniredis/v2 v2.32.1
	github.com/armon/go-metrics v0.4.1
	github.com/aws/aws-sdk-go v1.50.32
	github.com/bradfitz/gomemcache v0.0.0-20230905024940-24af94b03874
	github.com/cespare/xxhash v1.1.0
	github.com/cortexproject/promqlsmith v0.0.0-20240328172224-5e341f0dd08e
	github.com/dustin/go-humanize v1.0.1
	github.com/efficientgo/core v1.0.0-rc.2
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/felixge/fgprof v0.9.4
	github.com/go-kit/log v0.2.1
	github.com/go-openapi/strfmt v0.22.2
	github.com/go-openapi/swag v0.23.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.1
	github.com/golang-migrate/migrate/v4 v4.17.0
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v0.0.4
	github.com/gorilla/mux v1.8.1
	github.com/grafana/regexp v0.0.0-20221122212121-6b5c0a4cb7fd
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/hashicorp/consul/api v1.28.2
	github.com/hashicorp/go-cleanhttp v0.5.2
	github.com/hashicorp/go-sockaddr v1.0.6
	github.com/hashicorp/memberlist v0.5.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.17.8
	github.com/lib/pq v1.10.9
	github.com/minio/minio-go/v7 v7.0.69
	github.com/mitchellh/go-wordwrap v1.0.1
	github.com/oklog/ulid v1.3.1
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/alertmanager v0.27.0
	github.com/prometheus/client_golang v1.19.0
	github.com/prometheus/client_model v0.6.0
	github.com/prometheus/common v0.49.1-0.20240306132007-4199f18c3e92
	// Prometheus maps version 2.x.y to tags v0.x.y.
	github.com/prometheus/prometheus v0.51.1-0.20240328124728-d81e41d58ec8
	github.com/segmentio/fasthash v1.0.3
	github.com/sony/gobreaker v0.5.0
	github.com/spf13/afero v1.11.0
	github.com/stretchr/testify v1.9.0
	github.com/thanos-io/objstore v0.0.0-20240309075357-e8336a5fd5f3
	github.com/thanos-io/promql-engine v0.0.0-20240405095051-b7d0da367508
	github.com/thanos-io/thanos v0.34.2-0.20240423183430-7c8fe85682a5
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/weaveworks/common v0.0.0-20230728070032-dd9e68f319d5
	go.etcd.io/etcd/api/v3 v3.5.12
	go.etcd.io/etcd/client/pkg/v3 v3.5.13
	go.etcd.io/etcd/client/v3 v3.5.12
	go.opentelemetry.io/contrib/propagators/aws v1.22.0
	go.opentelemetry.io/otel v1.25.0
	go.opentelemetry.io/otel/bridge/opentracing v1.22.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.24.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.24.0
	go.opentelemetry.io/otel/sdk v1.25.0
	go.opentelemetry.io/otel/trace v1.25.0
	go.uber.org/atomic v1.11.0
	golang.org/x/net v0.24.0
	golang.org/x/sync v0.6.0
	golang.org/x/time v0.5.0
	google.golang.org/grpc v1.62.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/VictoriaMetrics/fastcache v1.12.1
	github.com/bboreham/go-loser v0.0.0-20230920113527-fcc2c21820a3
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/google/go-cmp v0.6.0
	github.com/sercand/kuberesolver/v4 v4.0.0
	go.opentelemetry.io/collector/pdata v1.3.0
	golang.org/x/exp v0.0.0-20240119083558-1b970713d09a
	google.golang.org/protobuf v1.33.0
)

require (
	cloud.google.com/go v0.112.0 // indirect
	cloud.google.com/go/compute v1.23.4 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.6 // indirect
	cloud.google.com/go/storage v1.36.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.10.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.5.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.5.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.3.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.2.1 // indirect
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/aws/aws-sdk-go-v2 v1.16.16 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.15.1 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.12.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.13.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.19 // indirect
	github.com/aws/smithy-go v1.13.3 // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/efficientgo/tools/extkingpin v0.0.0-20220817170617-6c25e3b627dd // indirect
	github.com/fatih/color v1.15.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/analysis v0.22.2 // indirect
	github.com/go-openapi/errors v0.21.1 // indirect
	github.com/go-openapi/jsonpointer v0.20.2 // indirect
	github.com/go-openapi/jsonreference v0.20.4 // indirect
	github.com/go-openapi/loads v0.21.5 // indirect
	github.com/go-openapi/runtime v0.27.1 // indirect
	github.com/go-openapi/spec v0.20.14 // indirect
	github.com/go-openapi/validate v0.23.0 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/gogo/googleapis v1.4.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/google/pprof v0.0.0-20240227163752-401108e1b7e7 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.0-rc.2.0.20201207153454-9f6bf00c00a7 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.19.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-hclog v1.5.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-version v1.6.0 // indirect
	github.com/hashicorp/golang-lru v0.6.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.6 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/metalmatze/signal v0.0.0-20210307161603-1c9aa721a97a // indirect
	github.com/miekg/dns v1.1.58 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/ncw/swift v1.0.53 // indirect
	github.com/oklog/run v1.1.0 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus-community/prom-label-proxy v0.8.1-0.20240127162815-c1195f9aabc0 // indirect
	github.com/prometheus/common/sigv4 v0.1.0 // indirect
	github.com/prometheus/exporter-toolkit v0.11.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/redis/rueidis v1.0.14-go1.18 // indirect
	github.com/rs/cors v1.10.1 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/shurcooL/httpfs v0.0.0-20230704072500-f1e31cf0ba5c // indirect
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/vimeo/galaxycache v0.0.0-20210323154928-b7e5d71c067a // indirect
	github.com/weaveworks/promrus v1.2.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	github.com/zhangyunhao116/umap v0.0.0-20221211160557-cb7705fafa39 // indirect
	go.mongodb.org/mongo-driver v1.14.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.3.0 // indirect
	go.opentelemetry.io/collector/semconv v0.96.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/contrib/propagators/autoprop v0.38.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.13.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.13.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.13.0 // indirect
	go.opentelemetry.io/otel/metric v1.25.0 // indirect
	go.opentelemetry.io/proto/otlp v1.1.0 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	go4.org/intern v0.0.0-20230525184215-6c62f75575cb // indirect
	go4.org/unsafe/assume-no-moving-gc v0.0.0-20230525183740-e7c30c78aeb2 // indirect
	golang.org/x/crypto v0.22.0 // indirect
	golang.org/x/mod v0.16.0 // indirect
	golang.org/x/oauth2 v0.18.0 // indirect
	golang.org/x/sys v0.19.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/tools v0.19.0 // indirect
	gonum.org/v1/gonum v0.12.0 // indirect
	google.golang.org/api v0.168.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20240205150955-31a09d347014 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240304212257-790db918fca8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240304161311-37d4d3c04a78 // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/telebot.v3 v3.2.1 // indirect
	k8s.io/apimachinery v0.29.3 // indirect
	k8s.io/client-go v0.29.3 // indirect
	k8s.io/klog/v2 v2.120.1 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
)

// Override since git.apache.org is down.  The docs say to fetch from github.
replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999

// Use fork of gocql that has gokit logs and Prometheus metrics.
replace github.com/gocql/gocql => github.com/grafana/gocql v0.0.0-20200605141915-ba5dc39ece85

// Replace memberlist with Grafana's fork which includes some fixes that haven't been merged upstream yet
replace github.com/hashicorp/memberlist => github.com/grafana/memberlist v0.3.1-0.20220714140823-09ffed8adbbe

// Same version being used by thanos
replace github.com/vimeo/galaxycache => github.com/thanos-community/galaxycache v0.0.0-20211122094458-3a32041a1f1e

// v0.20.5 caused go mod checksum error
replace github.com/go-openapi/spec => github.com/go-openapi/spec v0.20.6

// the v6.0.5851 Prometheus depends on doesn't seem to exist anymore?
replace github.com/ionos-cloud/sdk-go/v6 => github.com/ionos-cloud/sdk-go/v6 v6.0.4

replace github.com/google/gnostic => github.com/googleapis/gnostic v0.6.9

// Same replace used by thanos: (may be removed in the future)
// https://github.com/thanos-io/thanos/blob/fdeea3917591fc363a329cbe23af37c6fff0b5f0/go.mod#L265
replace gopkg.in/alecthomas/kingpin.v2 => github.com/alecthomas/kingpin v1.3.8-0.20210301060133-17f40c25f497

replace github.com/sercand/kuberesolver => github.com/sercand/kuberesolver/v5 v5.1.1

// Pin kuberesolver/v5 to support new grpc version. Need to upgrade kuberesolver version on weaveworks/common.
replace github.com/sercand/kuberesolver/v4 => github.com/sercand/kuberesolver/v5 v5.1.1
