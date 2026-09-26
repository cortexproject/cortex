package images

// If you change the image tag, remember to update it in the preloading done
// by GitHub actions (see .github/workflows/*).

// These are variables so that they can be modified.

var (
	Memcached  = "memcached:1.6.1"
	Redis      = "docker.io/redis:7.0.4-alpine"
	Minio      = "docker.io/cortexproject/minio:RELEASE.2024-07-04T14-25-45Z" // Unmodified copy of MinIO's official image; MinIO withdrew its public images.
	Consul     = "consul:1.8.4"
	ETCD       = "quay.io/coreos/etcd:v3.5.29"
	Prometheus = "quay.io/prometheus/prometheus:v3.9.1"
	Postgres   = "postgres:9.6.16"
)
