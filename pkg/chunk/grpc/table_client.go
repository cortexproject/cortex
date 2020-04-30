package grpc

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/chunk"
)

type TableClient struct {
	client GrpcStoreClient
}

// NewTableClient returns a new TableClient.
func NewTableClient(cfg Config) (*TableClient, error) {
	grpcClient, _, err := connectToGrpcServer(cfg.Address)
	if err != nil {
		return nil, err
	}
	client := &TableClient{
		client: grpcClient,
	}
	return client, nil
}

func (c *TableClient) ListTables(ctx context.Context) ([]string, error) {
	tables, err := c.client.ListTables(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return tables.TableNames, nil
}

func (c *TableClient) DeleteTable(ctx context.Context, name string) error {
	tableName := &TableName{TableName: name}
	_, err := c.client.DeleteTable(ctx, tableName)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *TableClient) DescribeTable(ctx context.Context, name string) (desc chunk.TableDesc, isActive bool, err error) {
	tableName := &TableName{TableName: name}
	tableDesc, err := c.client.DescribeTable(ctx, tableName)
	if err != nil {
		return desc, false, errors.WithStack(err)
	}
	desc.Name = tableDesc.Desc.Name
	desc.ProvisionedRead = tableDesc.Desc.ProvisionedRead
	desc.ProvisionedWrite = tableDesc.Desc.ProvisionedWrite
	desc.UseOnDemandIOMode = tableDesc.Desc.UseOnDemandIOMode
	desc.Tags = tableDesc.Desc.Tags
	return desc, tableDesc.IsActive, nil
}

func (c *TableClient) UpdateTable(ctx context.Context, current, expected chunk.TableDesc) error {
	currentTable := &TableDesc{}
	expectedTable := &TableDesc{}

	currentTable.Name = current.Name
	currentTable.UseOnDemandIOMode = current.UseOnDemandIOMode
	currentTable.ProvisionedWrite = current.ProvisionedWrite
	currentTable.ProvisionedRead = current.ProvisionedRead
	currentTable.Tags = current.Tags

	expectedTable.Name = expected.Name
	expectedTable.UseOnDemandIOMode = expected.UseOnDemandIOMode
	expectedTable.ProvisionedWrite = expected.ProvisionedWrite
	expectedTable.ProvisionedRead = expected.ProvisionedRead
	expectedTable.Tags = expected.Tags

	updateTableRequest := &UpdateTableRequest{
		Current:  currentTable,
		Expected: expectedTable,
	}
	_, err := c.client.UpdateTable(ctx, updateTableRequest)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *TableClient) CreateTable(ctx context.Context, desc chunk.TableDesc) error {
	tableDesc := &TableDesc{}
	tableDesc.Name = desc.Name
	tableDesc.ProvisionedRead = desc.ProvisionedRead
	tableDesc.ProvisionedWrite = desc.ProvisionedWrite
	tableDesc.Tags = desc.Tags
	tableDesc.UseOnDemandIOMode = desc.UseOnDemandIOMode

	_, err := c.client.CreateTable(ctx, tableDesc)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
