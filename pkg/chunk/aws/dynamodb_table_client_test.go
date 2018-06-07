package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/applicationautoscaling"
	"github.com/aws/aws-sdk-go/service/applicationautoscaling/applicationautoscalingiface"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/mtime"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	tablePrefix      = "cortex_"
	chunkTablePrefix = "chunks_"
	tablePeriod      = 7 * 24 * time.Hour
	gracePeriod      = 15 * time.Minute
	maxChunkAge      = 12 * time.Hour
	inactiveWrite    = 1
	inactiveRead     = 2
	write            = 200
	read             = 100
)

func expectedBaseTable(name string, provisionedRead, provisionedWrite int64) []chunk.TableDesc {
	return []chunk.TableDesc{
		{
			Name:             name,
			ProvisionedRead:  provisionedRead,
			ProvisionedWrite: provisionedWrite,
		},
	}
}

func expectedStaticTables(base, num int, provisionedRead, provisionedWrite int64) []chunk.TableDesc {
	result := []chunk.TableDesc{}
	for i := base; i < base+num; i++ {
		result = append(result,
			chunk.TableDesc{
				Name:             tablePrefix + fmt.Sprint(i),
				ProvisionedRead:  provisionedRead,
				ProvisionedWrite: provisionedWrite,
			},
			chunk.TableDesc{
				Name:             chunkTablePrefix + fmt.Sprint(i),
				ProvisionedRead:  provisionedRead,
				ProvisionedWrite: provisionedWrite,
			},
		)
	}
	return result
}

func expectedAutoscaledTables(base, num int, provisionedRead, provisionedWrite int64, indexOutCooldown int64, chunkTarget float64) []chunk.TableDesc {
	result := []chunk.TableDesc{}
	for i := base; i < num+base; i++ {
		result = append(result,
			chunk.TableDesc{
				Name:             tablePrefix + fmt.Sprint(i),
				ProvisionedRead:  provisionedRead,
				ProvisionedWrite: provisionedWrite,
				WriteScale: chunk.AutoScalingConfig{
					Enabled:     true,
					MinCapacity: 10,
					MaxCapacity: 20,
					OutCooldown: indexOutCooldown,
					InCooldown:  100,
					TargetValue: 80.0,
				},
			},
			chunk.TableDesc{
				Name:             chunkTablePrefix + fmt.Sprint(i),
				ProvisionedRead:  provisionedRead,
				ProvisionedWrite: provisionedWrite,
				WriteScale: chunk.AutoScalingConfig{
					Enabled:     true,
					MinCapacity: 10,
					MaxCapacity: 20,
					OutCooldown: 100,
					InCooldown:  100,
					TargetValue: chunkTarget,
				},
			},
		)
	}
	return result
}

func TestTableManagerAutoScaling(t *testing.T) {
	dynamoDB := newMockDynamoDB(0, 0)
	applicationAutoScaling := newMockApplicationAutoScaling()
	client := dynamoTableClient{
		DynamoDB:               dynamoDB,
		ApplicationAutoScaling: applicationAutoScaling,
	}

	test := func(tableManager *chunk.TableManager, name string, tm time.Time, expected []chunk.TableDesc) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mtime.NowForce(tm)
			defer mtime.NowReset()
			if err := tableManager.SyncTables(ctx); err != nil {
				t.Fatal(err)
			}
			err := chunk.ExpectTables(ctx, client, expected)
			require.NoError(t, err)
		})
	}

	cfg := chunk.SchemaConfig{
		UsePeriodicTables: true,
		IndexTables: chunk.PeriodicTableConfig{
			Prefix: tablePrefix,
			Period: tablePeriod,
			From:   util.NewDayValue(model.TimeFromUnix(0)),
			ProvisionedWriteThroughput: write,
			ProvisionedReadThroughput:  read,
			InactiveWriteThroughput:    inactiveWrite,
			InactiveReadThroughput:     inactiveRead,
			WriteScale: chunk.AutoScalingConfig{
				Enabled:     true,
				MinCapacity: 10,
				MaxCapacity: 20,
				OutCooldown: 100,
				InCooldown:  100,
				TargetValue: 80.0,
			},
		},

		ChunkTables: chunk.PeriodicTableConfig{
			Prefix: chunkTablePrefix,
			Period: tablePeriod,
			From:   util.NewDayValue(model.TimeFromUnix(0)),
			ProvisionedWriteThroughput: write,
			ProvisionedReadThroughput:  read,
			InactiveWriteThroughput:    inactiveWrite,
			InactiveReadThroughput:     inactiveRead,
			WriteScale: chunk.AutoScalingConfig{
				Enabled:     true,
				MinCapacity: 10,
				MaxCapacity: 20,
				OutCooldown: 100,
				InCooldown:  100,
				TargetValue: 80.0,
			},
		},

		CreationGracePeriod: gracePeriod,
	}

	// Check tables are created with autoscale
	{
		tableManager, err := chunk.NewTableManager(cfg, maxChunkAge, client)
		if err != nil {
			t.Fatal(err)
		}

		test(
			tableManager,
			"Create tables",
			time.Unix(0, 0).Add(maxChunkAge).Add(gracePeriod),
			append(expectedBaseTable("", inactiveRead, inactiveWrite),
				expectedAutoscaledTables(0, 1, read, write, 100, 80)...),
		)
	}

	// Check tables are updated with new settings
	{
		cfg.IndexTables.WriteScale.OutCooldown = 200
		cfg.ChunkTables.WriteScale.TargetValue = 90.0

		tableManager, err := chunk.NewTableManager(cfg, maxChunkAge, client)
		if err != nil {
			t.Fatal(err)
		}

		test(
			tableManager,
			"Update tables with new settings",
			time.Unix(0, 0).Add(maxChunkAge).Add(gracePeriod),
			append(expectedBaseTable("", inactiveRead, inactiveWrite),
				expectedAutoscaledTables(0, 1, read, write, 200, 90)...),
		)
	}

	// Check tables are degristered when autoscaling is disabled for inactive tables
	{
		cfg.IndexTables.WriteScale.OutCooldown = 200
		cfg.ChunkTables.WriteScale.TargetValue = 90.0

		tableManager, err := chunk.NewTableManager(cfg, maxChunkAge, client)
		if err != nil {
			t.Fatal(err)
		}

		test(
			tableManager,
			"Update tables with new settings",
			time.Unix(0, 0).Add(tablePeriod).Add(maxChunkAge).Add(gracePeriod),
			append(append(expectedBaseTable("", inactiveRead, inactiveWrite),
				expectedStaticTables(0, 1, inactiveRead, inactiveWrite)...),
				expectedAutoscaledTables(1, 1, read, write, 200, 90)...),
		)
	}

	// Check tables are degristered when autoscaling is disabled entirely
	{
		cfg.IndexTables.WriteScale.Enabled = false
		cfg.ChunkTables.WriteScale.Enabled = false

		tableManager, err := chunk.NewTableManager(cfg, maxChunkAge, client)
		if err != nil {
			t.Fatal(err)
		}

		test(
			tableManager,
			"Update tables with new settings",
			time.Unix(0, 0).Add(tablePeriod).Add(maxChunkAge).Add(gracePeriod),
			append(append(expectedBaseTable("", inactiveRead, inactiveWrite),
				expectedStaticTables(0, 1, inactiveRead, inactiveWrite)...),
				expectedStaticTables(1, 1, read, write)...),
		)
	}
}

func TestTableManagerInactiveAutoScaling(t *testing.T) {
	dynamoDB := newMockDynamoDB(0, 0)
	applicationAutoScaling := newMockApplicationAutoScaling()
	client := dynamoTableClient{
		DynamoDB:               dynamoDB,
		ApplicationAutoScaling: applicationAutoScaling,
	}

	test := func(tableManager *chunk.TableManager, name string, tm time.Time, expected []chunk.TableDesc) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mtime.NowForce(tm)
			defer mtime.NowReset()
			if err := tableManager.SyncTables(ctx); err != nil {
				t.Fatal(err)
			}
			err := chunk.ExpectTables(ctx, client, expected)
			require.NoError(t, err)
		})
	}

	cfg := chunk.SchemaConfig{
		UsePeriodicTables: true,
		IndexTables: chunk.PeriodicTableConfig{
			Prefix: tablePrefix,
			Period: tablePeriod,
			From:   util.NewDayValue(model.TimeFromUnix(0)),
			ProvisionedWriteThroughput: write,
			ProvisionedReadThroughput:  read,
			InactiveWriteThroughput:    inactiveWrite,
			InactiveReadThroughput:     inactiveRead,
			InactiveWriteScale: chunk.AutoScalingConfig{
				Enabled:     true,
				MinCapacity: 10,
				MaxCapacity: 20,
				OutCooldown: 100,
				InCooldown:  100,
				TargetValue: 80.0,
			},
			InactiveWriteScaleLastN: 2,
		},

		ChunkTables: chunk.PeriodicTableConfig{
			Prefix: chunkTablePrefix,
			Period: tablePeriod,
			From:   util.NewDayValue(model.TimeFromUnix(0)),
			ProvisionedWriteThroughput: write,
			ProvisionedReadThroughput:  read,
			InactiveWriteThroughput:    inactiveWrite,
			InactiveReadThroughput:     inactiveRead,
			InactiveWriteScale: chunk.AutoScalingConfig{
				Enabled:     true,
				MinCapacity: 10,
				MaxCapacity: 20,
				OutCooldown: 100,
				InCooldown:  100,
				TargetValue: 80.0,
			},
			InactiveWriteScaleLastN: 2,
		},

		CreationGracePeriod: gracePeriod,
	}

	// Check legacy and latest tables do not autoscale with inactive autoscale enabled.
	{
		tableManager, err := chunk.NewTableManager(cfg, maxChunkAge, client)
		if err != nil {
			t.Fatal(err)
		}

		test(
			tableManager,
			"Legacy and latest tables",
			time.Unix(0, 0).Add(maxChunkAge).Add(gracePeriod),
			append(expectedBaseTable("", inactiveRead, inactiveWrite),
				expectedStaticTables(0, 1, read, write)...),
		)
	}

	// Check inactive tables are autoscaled even if there are less than the limit.
	{
		tableManager, err := chunk.NewTableManager(cfg, maxChunkAge, client)
		if err != nil {
			t.Fatal(err)
		}

		test(
			tableManager,
			"1 week of inactive tables with latest",
			time.Unix(0, 0).Add(tablePeriod).Add(maxChunkAge).Add(gracePeriod),
			append(append(expectedBaseTable("", inactiveRead, inactiveWrite),
				expectedAutoscaledTables(0, 1, inactiveRead, inactiveWrite, 100, 80)...),
				expectedStaticTables(1, 1, read, write)...),
		)
	}

	// Check inactive tables past the limit do not autoscale but the latest N do.
	{
		tableManager, err := chunk.NewTableManager(cfg, maxChunkAge, client)
		if err != nil {
			t.Fatal(err)
		}

		test(
			tableManager,
			"3 weeks of inactive tables with latest",
			time.Unix(0, 0).Add(tablePeriod*3).Add(maxChunkAge).Add(gracePeriod),
			append(append(append(append(expectedBaseTable("", inactiveRead, inactiveWrite),
				expectedStaticTables(0, 1, inactiveRead, inactiveWrite)...),
				expectedAutoscaledTables(1, 1, inactiveRead, inactiveWrite, 100, 80)...),
				expectedAutoscaledTables(2, 1, inactiveRead, inactiveWrite, 100, 80)...),
				expectedStaticTables(3, 1, read, write)...),
		)
	}
}

type mockApplicationAutoScalingClient struct {
	applicationautoscalingiface.ApplicationAutoScalingAPI

	scalableTargets map[string]mockScalableTarget
	scalingPolicies map[string]mockScalingPolicy
}

type mockScalableTarget struct {
	RoleARN     string
	MinCapacity int64
	MaxCapacity int64
}

type mockScalingPolicy struct {
	ScaleInCooldown  int64
	ScaleOutCooldown int64
	TargetValue      float64
}

func newMockApplicationAutoScaling() *mockApplicationAutoScalingClient {
	return &mockApplicationAutoScalingClient{
		scalableTargets: map[string]mockScalableTarget{},
		scalingPolicies: map[string]mockScalingPolicy{},
	}
}

func (m *mockApplicationAutoScalingClient) RegisterScalableTarget(input *applicationautoscaling.RegisterScalableTargetInput) (*applicationautoscaling.RegisterScalableTargetOutput, error) {
	m.scalableTargets[*input.ResourceId] = mockScalableTarget{
		RoleARN:     *input.RoleARN,
		MinCapacity: *input.MinCapacity,
		MaxCapacity: *input.MaxCapacity,
	}
	return &applicationautoscaling.RegisterScalableTargetOutput{}, nil
}

func (m *mockApplicationAutoScalingClient) DeregisterScalableTarget(input *applicationautoscaling.DeregisterScalableTargetInput) (*applicationautoscaling.DeregisterScalableTargetOutput, error) {
	delete(m.scalableTargets, *input.ResourceId)
	return &applicationautoscaling.DeregisterScalableTargetOutput{}, nil
}

func (m *mockApplicationAutoScalingClient) DescribeScalableTargetsWithContext(ctx aws.Context, input *applicationautoscaling.DescribeScalableTargetsInput, options ...request.Option) (*applicationautoscaling.DescribeScalableTargetsOutput, error) {
	scalableTarget, ok := m.scalableTargets[*input.ResourceIds[0]]
	if !ok {
		return &applicationautoscaling.DescribeScalableTargetsOutput{}, nil
	}
	return &applicationautoscaling.DescribeScalableTargetsOutput{
		ScalableTargets: []*applicationautoscaling.ScalableTarget{
			{
				RoleARN:     aws.String(scalableTarget.RoleARN),
				MinCapacity: aws.Int64(scalableTarget.MinCapacity),
				MaxCapacity: aws.Int64(scalableTarget.MaxCapacity),
			},
		},
	}, nil
}

func (m *mockApplicationAutoScalingClient) PutScalingPolicy(input *applicationautoscaling.PutScalingPolicyInput) (*applicationautoscaling.PutScalingPolicyOutput, error) {
	m.scalingPolicies[*input.ResourceId] = mockScalingPolicy{
		ScaleInCooldown:  *input.TargetTrackingScalingPolicyConfiguration.ScaleInCooldown,
		ScaleOutCooldown: *input.TargetTrackingScalingPolicyConfiguration.ScaleOutCooldown,
		TargetValue:      *input.TargetTrackingScalingPolicyConfiguration.TargetValue,
	}
	return &applicationautoscaling.PutScalingPolicyOutput{}, nil
}

func (m *mockApplicationAutoScalingClient) DeleteScalingPolicy(input *applicationautoscaling.DeleteScalingPolicyInput) (*applicationautoscaling.DeleteScalingPolicyOutput, error) {
	delete(m.scalingPolicies, *input.ResourceId)
	return &applicationautoscaling.DeleteScalingPolicyOutput{}, nil
}

func (m *mockApplicationAutoScalingClient) DescribeScalingPoliciesWithContext(ctx aws.Context, input *applicationautoscaling.DescribeScalingPoliciesInput, options ...request.Option) (*applicationautoscaling.DescribeScalingPoliciesOutput, error) {
	scalingPolicy, ok := m.scalingPolicies[*input.ResourceId]
	if !ok {
		return &applicationautoscaling.DescribeScalingPoliciesOutput{}, nil
	}
	return &applicationautoscaling.DescribeScalingPoliciesOutput{
		ScalingPolicies: []*applicationautoscaling.ScalingPolicy{
			{
				TargetTrackingScalingPolicyConfiguration: &applicationautoscaling.TargetTrackingScalingPolicyConfiguration{
					ScaleInCooldown:  aws.Int64(scalingPolicy.ScaleInCooldown),
					ScaleOutCooldown: aws.Int64(scalingPolicy.ScaleOutCooldown),
					TargetValue:      aws.Float64(scalingPolicy.TargetValue),
				},
			},
		},
	}, nil
}
