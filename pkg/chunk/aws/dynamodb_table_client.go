package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/applicationautoscaling"
	"github.com/aws/aws-sdk-go/service/applicationautoscaling/applicationautoscalingiface"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	autoScalingPolicyNamePrefix = "DynamoScalingPolicy_cortex_"
)

var applicationAutoScalingRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "application_autoscaling_request_duration_seconds",
	Help:      "Time spent doing ApplicationAutoScaling requests.",

	// AWS latency seems to range from a few ms to a few sec. So use 8 buckets
	// from 128us to 2s. TODO: Confirm that this is the case for ApplicationAutoScaling.
	Buckets: prometheus.ExponentialBuckets(0.000128, 4, 8),
}, []string{"operation", "status_code"})

func init() {
	prometheus.MustRegister(applicationAutoScalingRequestDuration)
}

type dynamoTableClient struct {
	DynamoDB               dynamodbiface.DynamoDBAPI
	ApplicationAutoScaling applicationautoscalingiface.ApplicationAutoScalingAPI
	limiter                *rate.Limiter
	backoffConfig          util.BackoffConfig
}

// NewDynamoDBTableClient makes a new DynamoTableClient.
func NewDynamoDBTableClient(cfg DynamoDBConfig) (chunk.TableClient, error) {
	dynamoDB, err := dynamoClientFromURL(cfg.DynamoDB.URL)
	if err != nil {
		return nil, err
	}

	var applicationAutoScaling applicationautoscalingiface.ApplicationAutoScalingAPI
	if cfg.ApplicationAutoScaling.URL != nil {
		session, err := awsSessionFromURL(cfg.ApplicationAutoScaling.URL)
		if err != nil {
			return nil, err
		}
		applicationAutoScaling = applicationautoscaling.New(session)
	}

	return dynamoTableClient{
		DynamoDB:               dynamoDB,
		ApplicationAutoScaling: applicationAutoScaling,
		limiter:                rate.NewLimiter(rate.Limit(cfg.APILimit), 1),
		backoffConfig:          cfg.backoffConfig,
	}, nil
}

func (d dynamoTableClient) backoffAndRetry(ctx context.Context, fn func(context.Context) error) error {
	if d.limiter != nil { // Tests will have a nil limiter.
		d.limiter.Wait(ctx)
	}

	backoff := util.NewBackoff(ctx, d.backoffConfig)
	for backoff.Ongoing() {
		if err := fn(ctx); err != nil {
			if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "ThrottlingException" {
				level.Warn(util.WithContext(ctx, util.Logger)).Log("msg", "got error, backing off and retrying", "err", err, "retry", backoff.NumRetries())
				backoff.Wait()
				continue
			} else {
				return err
			}
		}
		return nil
	}
	return backoff.Err()
}

func (d dynamoTableClient) ListTables(ctx context.Context) ([]string, error) {
	table := []string{}
	err := d.backoffAndRetry(ctx, func(ctx context.Context) error {
		return instrument.TimeRequestHistogram(ctx, "DynamoDB.ListTablesPages", dynamoRequestDuration, func(ctx context.Context) error {
			return d.DynamoDB.ListTablesPagesWithContext(ctx, &dynamodb.ListTablesInput{}, func(resp *dynamodb.ListTablesOutput, _ bool) bool {
				for _, s := range resp.TableNames {
					table = append(table, *s)
				}
				return true
			})
		})
	})
	return table, err
}

func chunkTagsToDynamoDB(ts chunk.Tags) []*dynamodb.Tag {
	if ts == nil {
		return nil
	}

	var result []*dynamodb.Tag
	for k, v := range ts {
		result = append(result, &dynamodb.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return result
}

func (d dynamoTableClient) CreateTable(ctx context.Context, desc chunk.TableDesc) error {
	var tableARN *string
	if err := d.backoffAndRetry(ctx, func(ctx context.Context) error {
		return instrument.TimeRequestHistogram(ctx, "DynamoDB.CreateTable", dynamoRequestDuration, func(ctx context.Context) error {
			input := &dynamodb.CreateTableInput{
				TableName: aws.String(desc.Name),
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String(hashKey),
						AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
					},
					{
						AttributeName: aws.String(rangeKey),
						AttributeType: aws.String(dynamodb.ScalarAttributeTypeB),
					},
				},
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String(hashKey),
						KeyType:       aws.String(dynamodb.KeyTypeHash),
					},
					{
						AttributeName: aws.String(rangeKey),
						KeyType:       aws.String(dynamodb.KeyTypeRange),
					},
				},
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(desc.ProvisionedRead),
					WriteCapacityUnits: aws.Int64(desc.ProvisionedWrite),
				},
			}
			output, err := d.DynamoDB.CreateTableWithContext(ctx, input)
			if err != nil {
				return err
			}
			if output.TableDescription != nil {
				tableARN = output.TableDescription.TableArn
			}
			return nil
		})
	}); err != nil {
		return err
	}

	if desc.WriteScale.Enabled {
		err := d.enableAutoScaling(ctx, desc)
		if err != nil {
			return err
		}
	}

	tags := chunkTagsToDynamoDB(desc.Tags)
	if len(tags) > 0 {
		return d.backoffAndRetry(ctx, func(ctx context.Context) error {
			return instrument.TimeRequestHistogram(ctx, "DynamoDB.TagResource", dynamoRequestDuration, func(ctx context.Context) error {
				_, err := d.DynamoDB.TagResourceWithContext(ctx, &dynamodb.TagResourceInput{
					ResourceArn: tableARN,
					Tags:        tags,
				})
				return err
			})
		})
	}
	return nil
}

func (d dynamoTableClient) DescribeTable(ctx context.Context, name string) (desc chunk.TableDesc, status string, err error) {
	var tableARN *string
	err = d.backoffAndRetry(ctx, func(ctx context.Context) error {
		return instrument.TimeRequestHistogram(ctx, "DynamoDB.DescribeTable", dynamoRequestDuration, func(ctx context.Context) error {
			out, err := d.DynamoDB.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(name),
			})
			if err != nil {
				return err
			}
			desc.Name = name
			if out.Table != nil {
				if provision := out.Table.ProvisionedThroughput; provision != nil {
					if provision.ReadCapacityUnits != nil {
						desc.ProvisionedRead = *provision.ReadCapacityUnits
					}
					if provision.WriteCapacityUnits != nil {
						desc.ProvisionedWrite = *provision.WriteCapacityUnits
					}
				}
				if out.Table.TableStatus != nil {
					status = *out.Table.TableStatus
				}
				tableARN = out.Table.TableArn
			}
			return err
		})
	})
	if err != nil {
		return
	}

	err = d.backoffAndRetry(ctx, func(ctx context.Context) error {
		return instrument.TimeRequestHistogram(ctx, "DynamoDB.ListTagsOfResource", dynamoRequestDuration, func(ctx context.Context) error {
			out, err := d.DynamoDB.ListTagsOfResourceWithContext(ctx, &dynamodb.ListTagsOfResourceInput{
				ResourceArn: tableARN,
			})
			if err != nil {
				return err
			}
			desc.Tags = make(map[string]string, len(out.Tags))
			for _, tag := range out.Tags {
				desc.Tags[*tag.Key] = *tag.Value
			}
			return err
		})
	})

	if d.ApplicationAutoScaling != nil {
		err = d.backoffAndRetry(ctx, func(ctx context.Context) error {
			return instrument.TimeRequestHistogram(ctx, "ApplicationAutoScaling.DescribeScalableTargetsWithContext", applicationAutoScalingRequestDuration, func(ctx context.Context) error {
				out, err := d.ApplicationAutoScaling.DescribeScalableTargetsWithContext(ctx, &applicationautoscaling.DescribeScalableTargetsInput{
					ResourceIds:       []*string{aws.String("table/" + desc.Name)},
					ScalableDimension: aws.String("dynamodb:table:WriteCapacityUnits"),
					ServiceNamespace:  aws.String("dynamodb"),
				})
				if err != nil {
					return err
				}
				switch l := len(out.ScalableTargets); l {
				case 0:
					return err
				case 1:
					desc.WriteScale.Enabled = true
					if target := out.ScalableTargets[0]; target != nil {
						if target.RoleARN != nil {
							desc.WriteScale.RoleARN = *target.RoleARN
						}
						if target.MinCapacity != nil {
							desc.WriteScale.MinCapacity = *target.MinCapacity
						}
						if target.MaxCapacity != nil {
							desc.WriteScale.MaxCapacity = *target.MaxCapacity
						}
					}
					return err
				default:
					return fmt.Errorf("more than one scalable target found for DynamoDB table")
				}
			})
		})

		err = d.backoffAndRetry(ctx, func(ctx context.Context) error {
			return instrument.TimeRequestHistogram(ctx, "ApplicationAutoScaling.DescribeScalingPoliciesWithContext", applicationAutoScalingRequestDuration, func(ctx context.Context) error {
				out, err := d.ApplicationAutoScaling.DescribeScalingPoliciesWithContext(ctx, &applicationautoscaling.DescribeScalingPoliciesInput{
					PolicyNames:       []*string{aws.String(autoScalingPolicyNamePrefix + desc.Name)},
					ResourceId:        aws.String("table/" + desc.Name),
					ScalableDimension: aws.String("dynamodb:table:WriteCapacityUnits"),
					ServiceNamespace:  aws.String("dynamodb"),
				})
				if err != nil {
					return err
				}
				switch l := len(out.ScalingPolicies); l {
				case 0:
					return err
				case 1:
					config := out.ScalingPolicies[0].TargetTrackingScalingPolicyConfiguration
					if config != nil {
						if config.ScaleInCooldown != nil {
							desc.WriteScale.InCooldown = *config.ScaleInCooldown
						}
						if config.ScaleOutCooldown != nil {
							desc.WriteScale.OutCooldown = *config.ScaleOutCooldown
						}
						if config.TargetValue != nil {
							desc.WriteScale.TargetValue = *config.TargetValue
						}
					}
					return err
				default:
					return fmt.Errorf("more than one scaling policy found for DynamoDB table")
				}
			})
		})
	}
	return
}

func (d dynamoTableClient) UpdateTable(ctx context.Context, current, expected chunk.TableDesc) error {
	var err error
	if !current.WriteScale.Enabled {
		if expected.WriteScale.Enabled {
			err = d.enableAutoScaling(ctx, expected)
		}
	} else {
		if !expected.WriteScale.Enabled {
			err = d.disableAutoScaling(ctx, expected)
		} else if current.WriteScale != expected.WriteScale {
			err = d.enableAutoScaling(ctx, expected)
		}
	}
	if err != nil {
		return err
	}

	if current.ProvisionedRead != expected.ProvisionedRead || current.ProvisionedWrite != expected.ProvisionedWrite {
		if err := d.backoffAndRetry(ctx, func(ctx context.Context) error {
			return instrument.TimeRequestHistogram(ctx, "DynamoDB.UpdateTable", dynamoRequestDuration, func(ctx context.Context) error {
				_, err := d.DynamoDB.UpdateTableWithContext(ctx, &dynamodb.UpdateTableInput{
					TableName: aws.String(expected.Name),
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(expected.ProvisionedRead),
						WriteCapacityUnits: aws.Int64(expected.ProvisionedWrite),
					},
				})
				return err
			})
		}); err != nil {
			return err
		}
	}

	if !current.Tags.Equals(expected.Tags) {
		var tableARN *string
		if err := d.backoffAndRetry(ctx, func(ctx context.Context) error {
			return instrument.TimeRequestHistogram(ctx, "DynamoDB.DescribeTable", dynamoRequestDuration, func(ctx context.Context) error {
				out, err := d.DynamoDB.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String(expected.Name),
				})
				if err != nil {
					return err
				}
				if out.Table != nil {
					tableARN = out.Table.TableArn
				}
				return nil
			})
		}); err != nil {
			return err
		}

		return d.backoffAndRetry(ctx, func(ctx context.Context) error {
			return instrument.TimeRequestHistogram(ctx, "DynamoDB.TagResource", dynamoRequestDuration, func(ctx context.Context) error {
				_, err := d.DynamoDB.TagResourceWithContext(ctx, &dynamodb.TagResourceInput{
					ResourceArn: tableARN,
					Tags:        chunkTagsToDynamoDB(expected.Tags),
				})
				return err
			})
		})
	}
	return nil
}

func (d dynamoTableClient) enableAutoScaling(ctx context.Context, desc chunk.TableDesc) error {
	// Registers or updates a scalable target
	if err := d.backoffAndRetry(ctx, func(ctx context.Context) error {
		return instrument.TimeRequestHistogram(ctx, "ApplicationAutoScaling.RegisterScalableTarget", applicationAutoScalingRequestDuration, func(ctx context.Context) error {
			input := &applicationautoscaling.RegisterScalableTargetInput{
				MinCapacity:       aws.Int64(desc.WriteScale.MinCapacity),
				MaxCapacity:       aws.Int64(desc.WriteScale.MaxCapacity),
				ResourceId:        aws.String("table/" + desc.Name),
				RoleARN:           aws.String(desc.WriteScale.RoleARN),
				ScalableDimension: aws.String("dynamodb:table:WriteCapacityUnits"),
				ServiceNamespace:  aws.String("dynamodb"),
			}
			_, err := d.ApplicationAutoScaling.RegisterScalableTarget(input)
			if err != nil {
				return err
			}
			return nil
		})
	}); err != nil {
		return err
	}

	// Puts or updates a scaling policy
	return d.backoffAndRetry(ctx, func(ctx context.Context) error {
		return instrument.TimeRequestHistogram(ctx, "ApplicationAutoScaling.PutScalingPolicy", applicationAutoScalingRequestDuration, func(ctx context.Context) error {
			input := &applicationautoscaling.PutScalingPolicyInput{
				PolicyName:        aws.String(autoScalingPolicyNamePrefix + desc.Name),
				PolicyType:        aws.String("TargetTrackingScaling"),
				ResourceId:        aws.String("table/" + desc.Name),
				ScalableDimension: aws.String("dynamodb:table:WriteCapacityUnits"),
				ServiceNamespace:  aws.String("dynamodb"),
				TargetTrackingScalingPolicyConfiguration: &applicationautoscaling.TargetTrackingScalingPolicyConfiguration{
					PredefinedMetricSpecification: &applicationautoscaling.PredefinedMetricSpecification{
						PredefinedMetricType: aws.String("DynamoDBWriteCapacityUtilization"),
					},
					ScaleInCooldown:  aws.Int64(desc.WriteScale.InCooldown),
					ScaleOutCooldown: aws.Int64(desc.WriteScale.OutCooldown),
					TargetValue:      aws.Float64(desc.WriteScale.TargetValue),
				},
			}
			_, err := d.ApplicationAutoScaling.PutScalingPolicy(input)
			return err
		})
	})
}

func (d dynamoTableClient) disableAutoScaling(ctx context.Context, desc chunk.TableDesc) error {
	// Deregister scalable target
	if err := d.backoffAndRetry(ctx, func(ctx context.Context) error {
		return instrument.TimeRequestHistogram(ctx, "ApplicationAutoScaling.DeregisterScalableTarget", applicationAutoScalingRequestDuration, func(ctx context.Context) error {
			input := &applicationautoscaling.DeregisterScalableTargetInput{
				ResourceId:        aws.String("table/" + desc.Name),
				ScalableDimension: aws.String("dynamodb:table:WriteCapacityUnits"),
				ServiceNamespace:  aws.String("dynamodb"),
			}
			_, err := d.ApplicationAutoScaling.DeregisterScalableTarget(input)
			return err
		})
	}); err != nil {
		return err
	}

	// Delete scaling policy
	return d.backoffAndRetry(ctx, func(ctx context.Context) error {
		return instrument.TimeRequestHistogram(ctx, "ApplicationAutoScaling.DeleteScalingPolicy", applicationAutoScalingRequestDuration, func(ctx context.Context) error {
			input := &applicationautoscaling.DeleteScalingPolicyInput{
				PolicyName:        aws.String(autoScalingPolicyNamePrefix + desc.Name),
				ResourceId:        aws.String("table/" + desc.Name),
				ScalableDimension: aws.String("dynamodb:table:WriteCapacityUnits"),
				ServiceNamespace:  aws.String("dynamodb"),
			}
			_, err := d.ApplicationAutoScaling.DeleteScalingPolicy(input)
			return err
		})
	})
}
