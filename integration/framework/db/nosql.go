package e2edb

import (
	"net/url"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	awscommon "github.com/weaveworks/common/aws"

	e2e "github.com/cortexproject/cortex/integration/framework"
)

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

func NewDynamoDB() *e2e.Service {
	return e2e.NewService(
		"dynamodb",
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"amazon/dynamodb-local:1.11.477",
		e2e.NetworkName,
		[]int{8000},
		nil,
		e2e.NewCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"),
		// DynamoDB doesn't have a readiness probe, so we check if the / works even if returns 400
		e2e.NewReadinessProbe(8000, "/", 400),
	)
}
