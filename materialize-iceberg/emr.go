package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrTypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmTypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go/aws"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/materialize-iceberg/python"
	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
)

func clientCredSecretName(prefix string, materializationName string) string {
	return prefix + sanitizeAndAppendHash(materializationName)
}

func checkEmrPrereqs(ctx context.Context, d *materialization, errs *cerrors.PrereqErr) {
	if _, err := d.emrClient.ListJobRuns(ctx, &emr.ListJobRunsInput{
		ApplicationId: aws.String(d.cfg.Compute.ApplicationId),
		MaxResults:    aws.Int32(1),
	}); err != nil {
		errs.Err(fmt.Errorf("failed to list job runs for application %q: %w", d.cfg.Compute.ApplicationId, err))
	}

	if d.cfg.CatalogAuthentication.CatalogAuthType == catalogAuthTypeClientCredential {
		testParameter := clientCredSecretName(d.cfg.Compute.SystemsManagerPrefix, "test")
		if _, err := d.ssmClient.PutParameter(ctx, &ssm.PutParameterInput{
			Name:      aws.String(testParameter),
			Value:     aws.String("test"),
			Type:      ssmTypes.ParameterTypeSecureString,
			Overwrite: aws.Bool(true),
		}); err != nil {
			errs.Err(fmt.Errorf("failed to put secure string parameter to %s: %w", testParameter, err))
			return
		} else if _, err := d.ssmClient.GetParameter(ctx, &ssm.GetParameterInput{
			Name:           aws.String(testParameter),
			WithDecryption: aws.Bool(true),
		}); err != nil {
			errs.Err(fmt.Errorf("failed to get secure string parameter %s: %w", testParameter, err))
		}
	}
}

func ensureEmrSecret(ctx context.Context, client *ssm.Client, parameterName, wantCred string) error {
	res, err := client.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           aws.String(parameterName),
		WithDecryption: aws.Bool(true),
	})

	if err != nil {
		var errNotFound *ssmTypes.ParameterNotFound
		if !errors.As(err, &errNotFound) {
			return fmt.Errorf("error getting catalog credential parameter: %w", err)
		}
	}

	if res != nil && *res.Parameter.Value == wantCred {
		return nil
	}

	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      aws.String(parameterName),
		Value:     aws.String(wantCred),
		Type:      ssmTypes.ParameterTypeSecureString,
		Overwrite: aws.Bool(true),
	})
	if err != nil {
		return fmt.Errorf("error writing catalog credential parameter: %w", err)
	}

	log.Info("catalog credential parameter created")

	return nil
}

func (t *transactor) runEmrJob(ctx context.Context, jobName string, input any, workingPrefix, entryPointUri string) error {
	/***
	Available arguments to the pyspark script:
	| --input-uri              | Input for the program, as an s3 URI, to be parsed by the script        | Required |
	| --status-output          | Location where the final status object will be written.                | Required |
	| --catalog-url            | The catalog URL                                                        | Required |
	| --warehouse              | REST Warehouse                                                         | Required |
	| --region                 | EMR & SigV4 Region                                                     | Required |
	| --credential-secret-name | Name of the secret in Systems Manager if using client credentials auth | Optional |
	| --scope                  | Scope if using client credentials auth                                 | Optional |
	***/
	getStatus := func() (*python.StatusOutput, error) {
		var status python.StatusOutput
		statusKey := path.Join(workingPrefix, statusFile)
		if statusObj, err := t.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(t.cfg.Compute.Bucket),
			Key:    aws.String(statusKey),
		}); err != nil {
			return nil, fmt.Errorf("reading status object %q: %w", statusKey, err)
		} else if err := json.NewDecoder(statusObj.Body).Decode(&status); err != nil {
			return nil, fmt.Errorf("decoding status object %q: %w", statusKey, err)
		}
		return &status, nil
	}

	inputKey := path.Join(workingPrefix, "input.json")
	if inputBytes, err := encodeInput(input); err != nil {
		return fmt.Errorf("encoding input: %w", err)
	} else if _, err := t.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(t.cfg.Compute.Bucket),
		Key:    aws.String(inputKey),
		Body:   bytes.NewReader(inputBytes),
	}); err != nil {
		return fmt.Errorf("putting input file object: %w", err)
	}

	args := []string{
		"--input-uri", "s3://" + path.Join(t.cfg.Compute.Bucket, inputKey),
		"--status-output", "s3://" + path.Join(t.cfg.Compute.Bucket, workingPrefix, statusFile),
		"--catalog-url", t.cfg.URL,
		"--warehouse", t.cfg.Warehouse,
		"--region", t.cfg.Compute.Region,
	}

	if n := t.emrAuth.credentialSecretName; n != "" {
		args = append(args, "--credential-secret-name", n)
	}
	if s := t.emrAuth.scope; s != "" {
		args = append(args, "--scope", s)
	}

	start, err := t.emrClient.StartJobRun(ctx, &emr.StartJobRunInput{
		ApplicationId:    aws.String(t.cfg.Compute.ApplicationId),
		ClientToken:      aws.String(uuid.NewString()),
		ExecutionRoleArn: aws.String(t.cfg.Compute.ExecutionRoleArn),
		JobDriver: &emrTypes.JobDriverMemberSparkSubmit{
			Value: emrTypes.SparkSubmit{
				SparkSubmitParameters: aws.String(fmt.Sprintf("--py-files %s", t.pyFiles.commonURI)),
				EntryPoint:            aws.String(entryPointUri),
				EntryPointArguments:   args,
			},
		},
		Name: aws.String(jobName),
	})
	if err != nil {
		return err
	}

	var runDetails string
	for {
		gotRun, err := t.emrClient.GetJobRun(ctx, &emr.GetJobRunInput{
			ApplicationId: aws.String(t.cfg.Compute.ApplicationId),
			JobRunId:      start.JobRunId,
		})
		if err != nil {
			return err
		}
		runDetails = *gotRun.JobRun.StateDetails

		switch gotRun.JobRun.State {
		case emrTypes.JobRunStateSuccess:
			if status, err := getStatus(); err != nil {
				return fmt.Errorf("job succeeded but could not get final status: %w", err)
			} else if !status.Success {
				return fmt.Errorf("job failed ran successfully but had error: %s", status.Error)
			}

			return nil
		case emrTypes.JobRunStateFailed, emrTypes.JobRunStateCancelling, emrTypes.JobRunStateCancelled:
			if status, err := getStatus(); err != nil {
				return fmt.Errorf("job failed with no status output: %s", runDetails)
			} else {
				log.WithField("runDetails", runDetails).Error("emr job failed")
				return fmt.Errorf("job failed: %s", status.Error)
			}
		default:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func encodeInput(in any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetAppendNewline(false)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(in); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
