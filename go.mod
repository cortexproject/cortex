module github.com/cortexproject/cortex

go 1.12

require (
	cloud.google.com/go v0.28.0
	github.com/Masterminds/squirrel v0.0.0-20161115235646-20f192218cf5
	github.com/NYTimes/gziphandler v0.0.0-20190221231647-dd0439581c76
	github.com/OneOfOne/xxhash v1.2.5 // indirect
	github.com/armon/go-socks5 v0.0.0-20160902184237-e75332964ef5
	github.com/aws/aws-sdk-go v0.0.0-20181204211515-ddc06f9fad88
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/blang/semver v0.0.0-20170130170546-b38d23b8782a
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/bradfitz/gomemcache v0.0.0-20170208213004-1952afaa557d
	github.com/cenkalti/backoff v0.0.0-20160321131554-32cd0c5b3aef // indirect
	github.com/cznic/ql v1.2.0 // indirect
	github.com/dgrijalva/jwt-go v0.0.0-20180308231308-06ea1031745c // indirect
	github.com/etcd-io/bbolt v0.0.0-20180912205654-7ee3ded59d48
	github.com/fluent/fluent-logger-golang v0.0.0-20161018015219-28bdb662295c // indirect
	github.com/fsouza/fake-gcs-server v0.0.0-20180925145035-9f9efdc50d6a
	github.com/go-kit/kit v0.8.0
	github.com/gocql/gocql v0.0.0-20180113133114-697e7c57f99b
	github.com/gogo/googleapis v0.0.0-20180813112041-8558fb44d2f1 // indirect
	github.com/gogo/protobuf v1.2.0
	github.com/gogo/status v0.0.0-20180521094753-d60b5acac426
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/gofuzz v0.0.0-20161122191042-44d81051d367 // indirect
	github.com/google/martian v2.1.0+incompatible // indirect
	github.com/gophercloud/gophercloud v0.0.0-20190307220656-fe1ba5ce12dd // indirect
	github.com/gorilla/context v0.0.0-20160226214623-1ea25387ff6f // indirect
	github.com/gorilla/mux v1.6.2
	github.com/grpc-ecosystem/go-grpc-middleware v0.0.0-20180502091642-c250d6563d4d
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/consul v0.0.0-20180615161029-bed22a81e9fd
	github.com/hashicorp/go-cleanhttp v0.0.0-20170211013415-3573b8b52aa7
	github.com/hashicorp/serf v0.0.0-20170206200529-d6574a5bb122 // indirect
	github.com/jonboulle/clockwork v0.0.0-20160615175015-2eee05ed7941
	github.com/json-iterator/go v0.0.0-20180806060727-1624edc4454b
	github.com/kylelemons/godebug v0.0.0-20170820004349-d65d576e9348 // indirect
	github.com/lann/builder v0.0.0-20150808151131-f22ce00fd939 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/lib/pq v1.0.0
	github.com/mattes/migrate v0.0.0-20170420185049-7dde43471463
	github.com/mattn/go-colorable v0.0.0-20161103160040-d22884950486 // indirect
	github.com/mattn/go-isatty v0.0.0-20170322234413-fc9e8d8ef484 // indirect
	github.com/mattn/go-sqlite3 v1.10.0 // indirect
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing-contrib/go-stdlib v0.0.0-20170113013457-1de4cc2120e7
	github.com/opentracing/opentracing-go v1.0.1
	github.com/philhofer/fwd v0.0.0-20160129035939-98c11a7a6ec8 // indirect
	github.com/pkg/errors v0.8.0
	github.com/prometheus/alertmanager v0.0.0-20180112102915-fb713f6d8239
	github.com/prometheus/client_golang v0.9.1
	github.com/prometheus/common v0.0.0-20181119215939-b36ad289a3ea
	github.com/prometheus/prometheus v0.0.0-20190312040920-59369491cfdf
	github.com/prometheus/tsdb v0.6.1
	github.com/segmentio/fasthash v0.0.0-20180216231524-a72b379d632e
	github.com/sercand/kuberesolver v0.0.0-20171128105722-aa801ca26294 // indirect
	github.com/sirupsen/logrus v0.0.0-20180721070001-3e01752db018 // indirect
	github.com/stretchr/testify v1.3.0
	github.com/tinylib/msgp v0.0.0-20161221055906-38a6f61a768d // indirect
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v0.0.0-20180430180415-b043381d9447
	github.com/uber/jaeger-lib v0.0.0-20180511153330-ed3a127ec5fe // indirect
	github.com/weaveworks/billing-client v0.0.0-20171006123215-be0d55e547b1
	github.com/weaveworks/common v0.0.0-20181109173936-c1808abf9c46
	github.com/weaveworks/mesh v0.0.0-20170131170447-5015f896ab62
	github.com/weaveworks/promrus v0.0.0-20171010152908-0599d764e054 // indirect
	go.etcd.io/bbolt v1.3.2 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	golang.org/x/net v0.0.0-20181220203305-927f97764cc3
	golang.org/x/oauth2 v0.0.0-20181203162652-d668ce993890 // indirect
	golang.org/x/sys v0.0.0-20190209173611-3b5209105503
	golang.org/x/time v0.0.0-20170424234030-8be79e1e0910
	golang.org/x/tools v0.0.0-20190114222345-bf090417da8b
	google.golang.org/api v0.0.0-20181203233308-6142e720c068
	google.golang.org/grpc v1.19.0
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	gopkg.in/yaml.v2 v2.2.2
)
