package e2edb

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	awscommon "github.com/weaveworks/common/aws"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/integration/e2e/images"
)

const (
	MinioAccessKey = "Cheescake"
	MinioSecretKey = "supersecret"
)

// NewMinio returns minio server, used as a local replacement for S3.
func NewMinio(port int, bktName string) *e2e.HTTPService {
	minioKESGithubContent := "https://raw.githubusercontent.com/minio/kes/master"
	commands := []string{
		"curl -sSL --tlsv1.2 -O '%s/root.key'	-O '%s/root.cert'",
		"mkdir -p /data/%s && minio server --address :%v --quiet /data",
	}

	m := e2e.NewHTTPService(
		fmt.Sprintf("minio-%v", port),
		images.Minio,
		// Create the "cortex" bucket before starting minio
		e2e.NewCommandWithoutEntrypoint("sh", "-c", fmt.Sprintf(strings.Join(commands, " && "), minioKESGithubContent, minioKESGithubContent, bktName, port)),
		e2e.NewHTTPReadinessProbe(port, "/minio/health/ready", 200, 200),
		port,
	)
	m.SetEnvVars(map[string]string{
		"MINIO_ACCESS_KEY": MinioAccessKey,
		"MINIO_SECRET_KEY": MinioSecretKey,
		"MINIO_BROWSER":    "off",
		"ENABLE_HTTPS":     "0",
		// https://docs.min.io/docs/minio-kms-quickstart-guide.html
		"MINIO_KMS_KES_ENDPOINT":  "https://play.min.io:7373",
		"MINIO_KMS_KES_KEY_FILE":  "root.key",
		"MINIO_KMS_KES_CERT_FILE": "root.cert",
		"MINIO_KMS_KES_KEY_NAME":  "my-minio-key",
	})
	return m
}

func NewConsul() *e2e.HTTPService {
	return e2e.NewHTTPService(
		"consul",
		images.Consul,
		// Run consul in "dev" mode so that the initial leader election is immediate
		e2e.NewCommand("agent", "-server", "-client=0.0.0.0", "-dev", "-log-level=err"),
		e2e.NewHTTPReadinessProbe(8500, "/v1/operator/autopilot/health", 200, 200, `"Healthy": true`),
		8500,
	)
}

func NewETCD() *e2e.HTTPService {
	return e2e.NewHTTPService(
		"etcd",
		images.ETCD,
		e2e.NewCommand("/usr/local/bin/etcd", "--listen-client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://0.0.0.0:2379", "--listen-metrics-urls=http://0.0.0.0:9000", "--log-level=error"),
		e2e.NewHTTPReadinessProbe(9000, "/health", 200, 204),
		2379,
		9000, // Metrics
	)
}

func NewDynamoClient(endpoint string) (*dynamodb.DynamoDB, error) {
	dynamoURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	dynamoConfig, err := awscommon.ConfigFromURL(dynamoURL)
	if err != nil {
		return nil, err
	}

	dynamoConfig = dynamoConfig.WithMaxRetries(0)
	dynamoSession, err := session.NewSession(dynamoConfig)
	if err != nil {
		return nil, err
	}

	return dynamodb.New(dynamoSession), nil
}

func NewDynamoDB() *e2e.HTTPService {
	return e2e.NewHTTPService(
		"dynamodb",
		images.DynamoDB,
		e2e.NewCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"),
		// DynamoDB doesn't have a readiness probe, so we check if the / works even if returns 400
		e2e.NewHTTPReadinessProbe(8000, "/", 400, 400),
		8000,
	)
}

// while using Bigtable emulator as index store make sure you set BIGTABLE_EMULATOR_HOST environment variable to host:9035 for all the services which access stores
func NewBigtable() *e2e.HTTPService {
	return e2e.NewHTTPService(
		"bigtable",
		images.BigtableEmulator,
		nil,
		nil,
		9035,
	)
}

func NewCassandra() *e2e.HTTPService {
	return e2e.NewHTTPService(
		"cassandra",
		images.Cassandra,
		nil,
		// readiness probe inspired from https://github.com/kubernetes/examples/blob/b86c9d50be45eaf5ce74dee7159ce38b0e149d38/cassandra/image/files/ready-probe.sh
		e2e.NewCmdReadinessProbe(e2e.NewCommand("bash", "-c", "nodetool status | grep UN")),
		9042,
	)
}

func NewSwiftStorage() *e2e.HTTPService {
	return e2e.NewHTTPService(
		"swift",
		images.SwiftEmulator,
		nil,
		e2e.NewHTTPReadinessProbe(8080, "/", 404, 404),
		8080,
	)
}
