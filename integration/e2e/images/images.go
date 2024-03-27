package images

// If you change the image tag, remember to update it in the preloading done
// by GitHub actions (see .github/workflows/*).

// These are variables so that they can be modified.

var (
	Memcached  = "memcached:1.6.1"
	Redis      = "docker.io/redis:7.0.4-alpine"
	Minio      = "minio/minio:RELEASE.2021-10-13T00-23-17Z"
	Consul     = "consul:1.8.4"
	ETCD       = "gcr.io/etcd-development/etcd:v3.4.7"
	Prometheus = "quay.io/prometheus/prometheus:v2.51.0"
)
