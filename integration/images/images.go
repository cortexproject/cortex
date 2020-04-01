package images

// If you change the image tag, remember to update it in the preloading done
// by CircleCI too (see .circleci/config.yml).
const (
	Cortex_0_6_0 = "quay.io/cortexproject/cortex:v0.6.0"
	Cortex_0_7_0 = "quay.io/cortexproject/cortex:v0.7.0"

	Memcached        = "memcached:1.6.1"
	Minio            = "minio/minio:RELEASE.2019-12-30T05-45-39Z"
	Consul           = "consul:0.9"
	DynamoDB         = "amazon/dynamodb-local:1.11.477"
	BigtableEmulator = "shopify/bigtable-emulator:0.1.0"
	Cassandra        = "rinscy/cassandra:3.11.0"
)
