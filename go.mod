module github.com/cortexproject/cortex

require (
	cloud.google.com/go v0.28.0
	github.com/Azure/go-autorest v10.14.0+incompatible // indirect
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v1.1.1
	github.com/armon/go-socks5 v0.0.0-20160902184237-e75332964ef5
	github.com/aws/aws-sdk-go v1.15.90
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/blang/semver v3.5.0+incompatible
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/bradfitz/gomemcache v0.0.0-20170208213004-1952afaa557d
	github.com/cenkalti/backoff v1.0.0 // indirect
	github.com/cznic/ql v1.2.0 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/etcd-io/bbolt v1.3.1-etcd.8
	github.com/fluent/fluent-logger-golang v1.2.1 // indirect
	github.com/fsouza/fake-gcs-server v1.3.0
	github.com/go-kit/kit v0.8.0
	github.com/gocql/gocql v0.0.0-20180113133114-697e7c57f99b
	github.com/gogo/googleapis v1.1.0 // indirect
	github.com/gogo/protobuf v1.1.1
	github.com/gogo/status v1.0.3
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/gofuzz v0.0.0-20161122191042-44d81051d367 // indirect
	github.com/google/martian v2.1.0+incompatible // indirect
	github.com/googleapis/gnostic v0.2.0 // indirect
	github.com/gophercloud/gophercloud v0.0.0-20170916161221-b4c2377fa779 // indirect
	github.com/gorilla/context v0.0.0-20160226214623-1ea25387ff6f // indirect
	github.com/gorilla/mux v1.6.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/consul v1.1.0
	github.com/hashicorp/go-cleanhttp v0.0.0-20170211013415-3573b8b52aa7
	github.com/hashicorp/serf v0.8.1 // indirect
	github.com/jonboulle/clockwork v0.1.0
	github.com/json-iterator/go v1.1.5
	github.com/julienschmidt/httprouter v1.1.0 // indirect
	github.com/kylelemons/godebug v0.0.0-20170820004349-d65d576e9348 // indirect
	github.com/lann/builder v0.0.0-20150808151131-f22ce00fd939 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/lib/pq v1.0.0
	github.com/mattes/migrate v1.3.1
	github.com/mattn/go-colorable v0.0.7 // indirect
	github.com/mattn/go-isatty v0.0.2 // indirect
	github.com/mattn/go-sqlite3 v1.10.0 // indirect
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b
	github.com/oklog/oklog v0.2.2 // indirect
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing-contrib/go-stdlib v0.0.0-20170113013457-1de4cc2120e7
	github.com/opentracing/opentracing-go v1.0.2
	github.com/peterbourgon/diskv v2.0.1+incompatible // indirect
	github.com/philhofer/fwd v0.0.0-20160129035939-98c11a7a6ec8 // indirect
	github.com/pkg/errors v0.8.0
	github.com/prometheus/alertmanager v0.13.0
	github.com/prometheus/client_golang v0.9.1
	github.com/prometheus/common v0.0.0-20181119215939-b36ad289a3ea
	github.com/prometheus/prometheus v0.0.0-20190103120706-121603c417e3
	github.com/prometheus/tsdb v0.3.1
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sercand/kuberesolver v1.0.0 // indirect
	github.com/sirupsen/logrus v1.0.6 // indirect
	github.com/stretchr/testify v1.3.0
	github.com/tinylib/msgp v0.0.0-20161221055906-38a6f61a768d // indirect
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v2.14.0+incompatible
	github.com/uber/jaeger-lib v1.5.0 // indirect
	github.com/weaveworks/billing-client v0.0.0-20171006123215-be0d55e547b1
	github.com/weaveworks/common v0.0.0-20181109173936-c1808abf9c46
	github.com/weaveworks/mesh v0.0.0-20170131170447-5015f896ab62
	github.com/weaveworks/promrus v1.2.0 // indirect
	go.etcd.io/bbolt v1.3.2 // indirect
	go.opencensus.io v0.18.0 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	golang.org/x/net v0.0.0-20181114220301-adae6a3d119a
	golang.org/x/oauth2 v0.0.0-20181203162652-d668ce993890 // indirect
	golang.org/x/sys v0.0.0-20181119195503-ec83556a53fe
	golang.org/x/time v0.0.0-20170424234030-8be79e1e0910
	golang.org/x/tools v0.0.0-20181023010539-40a48ad93fbe
	google.golang.org/api v0.0.0-20181203233308-6142e720c068
	google.golang.org/grpc v1.16.0
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/fsnotify/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	gopkg.in/yaml.v2 v2.2.1
	k8s.io/api v0.0.0-20180809133242-91bfdbcf0c2c // indirect
)
