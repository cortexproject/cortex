package gcp

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"

	"cloud.google.com/go/bigtable"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/pkg/errors"
)

type tableClient struct {
	cfg    Config
	client *bigtable.AdminClient
}

// NewTableClient returns a new TableClient.
func NewTableClient(ctx context.Context, cfg Config) (chunk.TableClient, error) {
	opts := toOptions(cfg.GRPCClientConfig.DialOption(bigtableInstrumentation()))
	client, err := bigtable.NewAdminClient(ctx, cfg.Project, cfg.Instance, opts...)
	if err != nil {
		return nil, err
	}
	return &tableClient{
		cfg:    cfg,
		client: client,
	}, nil
}

func (c *tableClient) ListTables(ctx context.Context) ([]string, error) {
	tables, err := c.client.Tables(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "client.Tables")
	}

	// Check each table has the right column family.  If not, omit it.
	output := make([]string, 0, len(tables))
	for _, table := range tables {
		info, err := c.client.TableInfo(ctx, table)
		if err != nil {
			return nil, errors.Wrap(err, "client.TableInfo")
		}

		if hasColumnFamily(info.FamilyInfos) {
			output = append(output, table)
		}
	}

	return output, nil
}

func hasColumnFamily(infos []bigtable.FamilyInfo) bool {
	for _, family := range infos {
		if family.Name == columnFamily {
			return true
		}
	}
	return false
}

func (c *tableClient) CreateTable(ctx context.Context, desc chunk.TableDesc) error {
	splitKeys := []string{}
	for i := 0; i < 256; i++ {
		splitKeys = append(splitKeys, fmt.Sprintf("%02x", i))
	}

	if err := c.client.CreateTableFromConf(ctx, &bigtable.TableConf{
		TableID:   desc.Name,
		SplitKeys: splitKeys,
		Families: map[string]bigtable.GCPolicy{
			columnFamily: bigtable.NoGcPolicy(),
		},
	}); err != nil {
		if !alreadyExistsError(err) {
			return errors.Wrap(err, "client.CreateTable")
		}
	}
	return nil
}

func alreadyExistsError(err error) bool {
	serr, ok := status.FromError(err)
	return ok && serr.Code() == codes.AlreadyExists
}

func (c *tableClient) DeleteTable(ctx context.Context, name string) error {
	if err := c.client.DeleteTable(ctx, name); err != nil {
		return errors.Wrap(err, "client.DeleteTable")
	}

	return nil
}

func (c *tableClient) DescribeTable(ctx context.Context, name string) (desc chunk.TableDesc, isActive bool, err error) {
	return chunk.TableDesc{
		Name: name,
	}, true, nil
}

func (c *tableClient) UpdateTable(ctx context.Context, current, expected chunk.TableDesc) error {
	return nil
}
