package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	glueTypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	log "github.com/sirupsen/logrus"
)

// configureTableOptimizers creates or updates AWS Glue managed table optimizers
// for the given table. It is idempotent: enabled optimizers are created if they
// do not exist, or updated if they do.
func configureTableOptimizers(ctx context.Context, client *glue.Client, catalogID, ns, tableName string, cfg glueOptimizersConfig) error {
	type optimizerSpec struct {
		optType glueTypes.TableOptimizerType
		config  *glueTypes.TableOptimizerConfiguration
	}

	var specs []optimizerSpec
	roleArn := aws.String(cfg.ExecutionRoleArn)

	if cfg.EnableCompaction {
		specs = append(specs, optimizerSpec{
			optType: glueTypes.TableOptimizerTypeCompaction,
			config: &glueTypes.TableOptimizerConfiguration{
				Enabled: aws.Bool(true),
				RoleArn: roleArn,
			},
		})
	}

	if cfg.EnableRetention {
		icebergRetention := &glueTypes.IcebergRetentionConfiguration{}
		if cfg.SnapshotRetentionDays > 0 {
			v := int32(cfg.SnapshotRetentionDays)
			icebergRetention.SnapshotRetentionPeriodInDays = &v
		}
		if cfg.NumberOfSnapshotsToRetain > 0 {
			v := int32(cfg.NumberOfSnapshotsToRetain)
			icebergRetention.NumberOfSnapshotsToRetain = &v
		}
		specs = append(specs, optimizerSpec{
			optType: glueTypes.TableOptimizerTypeRetention,
			config: &glueTypes.TableOptimizerConfiguration{
				Enabled: aws.Bool(true),
				RoleArn: roleArn,
				RetentionConfiguration: &glueTypes.RetentionConfiguration{
					IcebergConfiguration: icebergRetention,
				},
			},
		})
	}

	if cfg.EnableOrphanFileDeletion {
		icebergOrphan := &glueTypes.IcebergOrphanFileDeletionConfiguration{}
		if cfg.OrphanFileRetentionDays > 0 {
			v := int32(cfg.OrphanFileRetentionDays)
			icebergOrphan.OrphanFileRetentionPeriodInDays = &v
		}
		specs = append(specs, optimizerSpec{
			optType: glueTypes.TableOptimizerTypeOrphanFileDeletion,
			config: &glueTypes.TableOptimizerConfiguration{
				Enabled: aws.Bool(true),
				RoleArn: roleArn,
				OrphanFileDeletionConfiguration: &glueTypes.OrphanFileDeletionConfiguration{
					IcebergConfiguration: icebergOrphan,
				},
			},
		})
	}

	for _, spec := range specs {
		ll := log.WithFields(log.Fields{
			"table":     fmt.Sprintf("%s.%s", ns, tableName),
			"optimizer": spec.optType,
		})

		_, err := client.GetTableOptimizer(ctx, &glue.GetTableOptimizerInput{
			CatalogId:    &catalogID,
			DatabaseName: &ns,
			TableName:    &tableName,
			Type:         spec.optType,
		})
		if err != nil {
			var notFound *glueTypes.EntityNotFoundException
			if !errors.As(err, &notFound) {
				return fmt.Errorf("checking %s optimizer for %s.%s: %w", spec.optType, ns, tableName, err)
			}

			ll.Debug("creating Glue table optimizer")
			if _, createErr := client.CreateTableOptimizer(ctx, &glue.CreateTableOptimizerInput{
				CatalogId:                   &catalogID,
				DatabaseName:                &ns,
				TableName:                   &tableName,
				Type:                        spec.optType,
				TableOptimizerConfiguration: spec.config,
			}); createErr != nil {
				return fmt.Errorf("creating %s optimizer for %s.%s: %w", spec.optType, ns, tableName, createErr)
			}
			ll.Info("created Glue table optimizer")
		} else {
			ll.Debug("updating Glue table optimizer")
			if _, updateErr := client.UpdateTableOptimizer(ctx, &glue.UpdateTableOptimizerInput{
				CatalogId:                   &catalogID,
				DatabaseName:                &ns,
				TableName:                   &tableName,
				Type:                        spec.optType,
				TableOptimizerConfiguration: spec.config,
			}); updateErr != nil {
				return fmt.Errorf("updating %s optimizer for %s.%s: %w", spec.optType, ns, tableName, updateErr)
			}
			ll.Info("updated Glue table optimizer")
		}
	}

	return nil
}
