package cassandra

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/gocql/gocql"

	"github.com/weaveworks/cortex/pkg/chunk"
)

type tableClient struct {
	cfg     Config
	session *gocql.Session
}

// NewTableClient returns a new TableClient.
func NewTableClient(ctx context.Context, cfg Config) (chunk.TableClient, error) {
	session, err := cfg.cluster().CreateSession()
	if err != nil {
		return nil, err
	}
	return &tableClient{
		cfg:     cfg,
		session: session,
	}, nil
}

func (c *tableClient) ListTables(ctx context.Context) ([]string, error) {
	iter := c.session.Query("DESCRIBE TABLES").WithContext(ctx).Iter()
	defer iter.Close()
	scanner := iter.Scanner()
	result := []string{}
	for scanner.Next() {
		var table string
		if err := scanner.Scan(&table); err != nil {
			return nil, err
		}
		result = append(result, table)
	}
	return result, scanner.Err()
}

func (c *tableClient) CreateTable(ctx context.Context, desc chunk.TableDesc) error {
	return c.session.Query(`
		CREATE TABLE IF NOT EXISTS ? (
			hash text,
			range blob,
			value blob,
			PRIMARY KEY (hash, range)
		)`, desc.Name).WithContext(ctx).Exec()
}

func (c *tableClient) DescribeTable(ctx context.Context, name string) (desc chunk.TableDesc, status string, err error) {
	return chunk.TableDesc{
		Name: name,
	}, dynamodb.TableStatusActive, nil
}

func (c *tableClient) UpdateTable(ctx context.Context, current, expected chunk.TableDesc) error {
	return nil
}
