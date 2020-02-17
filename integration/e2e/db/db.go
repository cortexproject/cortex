package e2edb

import (
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	awscommon "github.com/weaveworks/common/aws"

	"github.com/cortexproject/cortex/integration/e2e"
)

const (
	MinioAccessKey = "Cheescake"
	MinioSecretKey = "supersecret"
)

// NewMinio returns minio server, used as a local replacement for S3.
func NewMinio(port int, bktName string) *e2e.HTTPService {
	m := e2e.NewHTTPService(
		fmt.Sprintf("minio-%v", port),
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"minio/minio:RELEASE.2019-12-30T05-45-39Z",
		// Create the "cortex" bucket before starting minio
		e2e.NewCommandWithoutEntrypoint("sh", "-c", fmt.Sprintf("mkdir -p /data/%s && minio server --address :%v --quiet /data", bktName, port)),
		e2e.NewReadinessProbe(port, "/minio/health/ready", 200),
		port,
	)
	m.SetEnvVars(map[string]string{
		"MINIO_ACCESS_KEY": MinioAccessKey,
		"MINIO_SECRET_KEY": MinioSecretKey,
		"MINIO_BROWSER":    "off",
		"ENABLE_HTTPS":     "0",
	})
	return m
}

func NewConsul() *e2e.HTTPService {
	return e2e.NewHTTPService(
		"consul",
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"consul:0.9",
		// Run consul in "dev" mode so that the initial leader election is immediate
		e2e.NewCommand("agent", "-server", "-client=0.0.0.0", "-dev", "-log-level=err"),
		nil,
		8500,
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
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"amazon/dynamodb-local:1.11.477",
		e2e.NewCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"),
		// DynamoDB doesn't have a readiness probe, so we check if the / works even if returns 400
		e2e.NewReadinessProbe(8000, "/", 400),
		8000,
	)
}
